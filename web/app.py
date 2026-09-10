"""
FastAPI Real-Time Web Application & WebSocket Hub.

Demonstrates:
1. Chapter 3 Assignment: Avro Order Serialization, Real-Time Running Average,
   Transient Failure Retry Logic, and Dead Letter Queue (DLQ) Quarantine.
2. Interactive Chaos / Fault Injection: Real-time simulation of network timeouts and poison pills.
3. Smart Grid Kappa Pipeline: Real-time zone energy telemetry, net grid load (DRAW/FEED),
   and Airflow orchestrator health monitoring.
"""

import os
import sys
import json
import time
import random
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from avro_order_lab.order_producer_consumer import OrderStreamingSystem, PRODUCTS
from scripts.telemetry_producer import SmartGridTelemetrySimulator
from scripts.tariff_batch_generator import execute_batch_drop
from streaming.live_stream_engine import LiveKappaStreamProcessor, SQLITE_DB_PATH
from dags.smart_grid_dag import check_infrastructure_health, validate_tariff_data_quality

logger = settings.get_logger("web_server")

# ---------------------------------------------------------------------------
# Enhanced Order Streaming Engine with Detailed Audit Trail for Live UI
# ---------------------------------------------------------------------------
class WebOrderStreamingSystem(OrderStreamingSystem):
    def __init__(self, dlq_topic="order_dlq"):
        super().__init__(dlq_topic=dlq_topic)
        self.order_history: List[Dict[str, Any]] = []
        self.max_history = 50
        self.success_count = 0
        self.glitch_retry_count = 0

    def process_order_detailed(
        self,
        message: dict,
        force_glitch_attempts: int = 0,
        force_permanent_error: Optional[str] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Executes order message processing with a detailed step-by-step audit trail
        for real-time visualization on the web dashboard.
        """
        audit_trail = []
        start_time = time.time()
        order_id = message.get("orderId", "UNKNOWN")

        for attempt in range(1, max_retries + 1):
            try:
                # Schema validation
                if "orderId" not in message or "price" not in message:
                    raise ValueError("Malformed schema: missing required Avro fields ('orderId', 'price')")

                price = float(message["price"])
                if price < 0:
                    raise ValueError(f"Poison pill detected: negative price (${price:.2f})")

                if force_permanent_error:
                    raise ValueError(f"Forced poison pill: {force_permanent_error}")

                # Forced or random transient glitch
                if attempt <= force_glitch_attempts or (force_glitch_attempts == 0 and attempt == 1 and random.random() < 0.08):
                    raise IOError("Simulated transient socket timeout (Kafka broker ack timeout)")

                # Success path: calculate running average
                self.total_orders += 1
                self.success_count += 1
                self.cumulative_price += price
                running_avg = self.cumulative_price / self.total_orders

                audit_trail.append({
                    "attempt": attempt,
                    "status": "SUCCESS",
                    "note": f"Avro message validated and processed on attempt {attempt}"
                })

                record = {
                    "orderId": order_id,
                    "product": message.get("product", "Unknown"),
                    "price": price,
                    "status": "SUCCESS" if attempt == 1 else "RECOVERED_AFTER_RETRY",
                    "attempts": attempt,
                    "running_avg": round(running_avg, 2),
                    "total_orders": self.total_orders,
                    "audit_trail": audit_trail,
                    "timestamp": time.time(),
                    "duration_ms": round((time.time() - start_time) * 1000, 2)
                }

                self._record_history(record)
                return record

            except IOError as transient_err:
                self.glitch_retry_count += 1
                audit_trail.append({
                    "attempt": attempt,
                    "status": "RETRY",
                    "error": str(transient_err),
                    "backoff_sec": round(0.15 * attempt, 2)
                })
                # Simulate backoff wait
                time.sleep(0.05 * attempt)

            except Exception as unrecoverable_err:
                audit_trail.append({
                    "attempt": attempt,
                    "status": "FATAL",
                    "error": str(unrecoverable_err)
                })
                dlq_record = self._send_to_dlq_detailed(message, reason=str(unrecoverable_err), audit=audit_trail)
                record = {
                    "orderId": order_id,
                    "product": message.get("product", "Unknown"),
                    "price": message.get("price", 0),
                    "status": "DLQ_QUARANTINED",
                    "attempts": attempt,
                    "running_avg": round(self.cumulative_price / self.total_orders, 2) if self.total_orders else 0.0,
                    "total_orders": self.total_orders,
                    "audit_trail": audit_trail,
                    "dlq_record": dlq_record,
                    "timestamp": time.time(),
                    "duration_ms": round((time.time() - start_time) * 1000, 2)
                }
                self._record_history(record)
                return record

        # Max retries exhausted
        exhausted_reason = f"Max retries ({max_retries}) exceeded"
        audit_trail.append({
            "attempt": max_retries,
            "status": "EXHAUSTED",
            "error": exhausted_reason
        })
        dlq_record = self._send_to_dlq_detailed(message, reason=exhausted_reason, audit=audit_trail)
        record = {
            "orderId": order_id,
            "product": message.get("product", "Unknown"),
            "price": message.get("price", 0),
            "status": "DLQ_QUARANTINED",
            "attempts": max_retries,
            "running_avg": round(self.cumulative_price / self.total_orders, 2) if self.total_orders else 0.0,
            "total_orders": self.total_orders,
            "audit_trail": audit_trail,
            "dlq_record": dlq_record,
            "timestamp": time.time(),
            "duration_ms": round((time.time() - start_time) * 1000, 2)
        }
        self._record_history(record)
        return record

    def _send_to_dlq_detailed(self, message: dict, reason: str, audit: list) -> dict:
        dlq_id = f"DLQ-{len(self.dlq_store) + 1:04d}"
        dlq_record = {
            "dlq_id": dlq_id,
            "failed_message": message,
            "error_reason": reason,
            "audit_trail": audit,
            "timestamp": time.time(),
            "replayed": False
        }
        self.dlq_store.append(dlq_record)
        logger.warning(f"Message quarantined to DLQ: {dlq_id} | Reason: {reason}")
        return dlq_record

    def _record_history(self, record: dict):
        self.order_history.insert(0, record)
        if len(self.order_history) > self.max_history:
            self.order_history.pop()


# ---------------------------------------------------------------------------
# Global State & Background Streaming Coordinator
# ---------------------------------------------------------------------------
class PipelineHub:
    def __init__(self):
        self.order_system = WebOrderStreamingSystem()
        self.order_sequence = 1000

        # Smart Grid components
        self.telemetry_sim = SmartGridTelemetrySimulator(num_meters=settings.DEFAULT_NUM_METERS)
        self.tariff_file = execute_batch_drop(num_meters=settings.DEFAULT_NUM_METERS, day_offset=1)
        self.grid_processor = LiveKappaStreamProcessor(tariff_path=self.tariff_file)
        self.latest_grid_metrics: List[Dict[str, Any]] = []
        self.simulated_day = 1
        self.grid_batches = 0
        self.total_grid_events = 0

        # Auto-streaming control
        self.auto_stream_active = True
        self.stream_speed = 1.0  # seconds per tick

        # Connected WebSocket clients
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        dead_connections = []
        payload_str = json.dumps(message)
        for connection in self.active_connections:
            try:
                await connection.send_text(payload_str)
            except Exception:
                dead_connections.append(connection)

        for dead in dead_connections:
            self.disconnect(dead)


hub = PipelineHub()


# ---------------------------------------------------------------------------
# Background Task: Continuous Stream Ingestion & Broadcast
# ---------------------------------------------------------------------------
async def continuous_pipeline_loop():
    logger.info("Starting background continuous streaming pipeline loop...")
    last_tariff_drop = time.time()

    while True:
        try:
            if hub.auto_stream_active:
                # 1. Process 1 Avro Order
                hub.order_sequence += 1
                order_msg = hub.order_system.generate_simulated_order(hub.order_sequence)
                order_res = hub.order_system.process_order_detailed(order_msg)

                # 2. Process Smart Grid Telemetry Batch
                grid_events = hub.telemetry_sim.generate_telemetry_batch()
                hub.grid_batches += 1
                hub.total_grid_events += len(grid_events)
                zone_metrics = hub.grid_processor.process_telemetry_batch(grid_events)
                hub.latest_grid_metrics = zone_metrics

                # 3. Check for simulated day tariff update
                if time.time() - last_tariff_drop >= settings.SIMULATED_DAY_DURATION_SEC:
                    hub.simulated_day += 1
                    hub.tariff_file = execute_batch_drop(
                        num_meters=settings.DEFAULT_NUM_METERS,
                        day_offset=hub.simulated_day
                    )
                    hub.grid_processor.refresh_tariffs()
                    last_tariff_drop = time.time()

                # 4. Broadcast update over WebSockets
                broadcast_data = {
                    "type": "STREAM_TICK",
                    "order": order_res,
                    "order_stats": {
                        "total_orders": hub.order_system.total_orders,
                        "running_avg": round(
                            hub.order_system.cumulative_price / hub.order_system.total_orders, 2
                        ) if hub.order_system.total_orders else 0.0,
                        "success_count": hub.order_system.success_count,
                        "retries_count": hub.order_system.glitch_retry_count,
                        "dlq_count": len(hub.order_system.dlq_store)
                    },
                    "grid": {
                        "simulated_day": hub.simulated_day,
                        "batch_num": hub.grid_batches,
                        "total_events": hub.total_grid_events,
                        "zones": zone_metrics,
                        "db_sink": "PostgreSQL" if hub.grid_processor.db_sink.use_postgres else "SQLite"
                    }
                }
                await hub.broadcast(broadcast_data)

            # Sleep according to configured cadence
            await asyncio.sleep(max(0.5, hub.stream_speed))

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in background streaming loop: {e}")
            await asyncio.sleep(1.0)


# ---------------------------------------------------------------------------
# FastAPI Lifespan Manager
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: spawn streaming task
    task = asyncio.create_task(continuous_pipeline_loop())
    yield
    # Shutdown: cancel task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# FastAPI App Initialization
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Real-Time Streaming Operations Center",
    description="Live Kappa Data Pipeline & Chapter 3 Avro DLQ Lab Demonstration Web UI",
    version="2.0.0",
    lifespan=lifespan
)

# Mount Static Files
STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# REST Models
# ---------------------------------------------------------------------------
class CustomOrderRequest(BaseModel):
    product: Optional[str] = "Item1"
    price: Optional[float] = 49.99

class ReplayDLQRequest(BaseModel):
    dlq_id: str
    corrected_price: Optional[float] = None


# ---------------------------------------------------------------------------
# Routes: Web UI & WebSocket
# ---------------------------------------------------------------------------
@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return HTMLResponse("<h1>Dashboard initializing... Please refresh shortly.</h1>")
    return FileResponse(str(index_path))


@app.websocket("/ws/stream")
async def websocket_stream_endpoint(websocket: WebSocket):
    await hub.connect(websocket)
    try:
        # Send immediate initial state
        initial_state = {
            "type": "INIT_STATE",
            "order_stats": {
                "total_orders": hub.order_system.total_orders,
                "running_avg": round(
                    hub.order_system.cumulative_price / hub.order_system.total_orders, 2
                ) if hub.order_system.total_orders else 0.0,
                "success_count": hub.order_system.success_count,
                "retries_count": hub.order_system.glitch_retry_count,
                "dlq_count": len(hub.order_system.dlq_store)
            },
            "recent_orders": hub.order_system.order_history[:20],
            "dlq_items": hub.order_system.dlq_store[-20:],
            "grid": {
                "simulated_day": hub.simulated_day,
                "batch_num": hub.grid_batches,
                "total_events": hub.total_grid_events,
                "zones": hub.latest_grid_metrics,
                "db_sink": "PostgreSQL" if hub.grid_processor.db_sink.use_postgres else "SQLite"
            },
            "auto_stream": hub.auto_stream_active,
            "stream_speed": hub.stream_speed
        }
        await websocket.send_text(json.dumps(initial_state))

        while True:
            # Keep socket alive and accept client commands
            data = await websocket.receive_text()
            cmd = json.loads(data)
            action = cmd.get("action")
            if action == "PING":
                await websocket.send_text(json.dumps({"type": "PONG"}))

    except WebSocketDisconnect:
        hub.disconnect(websocket)
    except Exception as e:
        logger.warning(f"WebSocket exception: {e}")
        hub.disconnect(websocket)


# ---------------------------------------------------------------------------
# REST Endpoints: Status & Health
# ---------------------------------------------------------------------------
@app.get("/api/status")
def get_system_status():
    """Returns comprehensive system metrics for health audits."""
    infra = check_infrastructure_health()
    dq = validate_tariff_data_quality()

    running_avg = (
        hub.order_system.cumulative_price / hub.order_system.total_orders
        if hub.order_system.total_orders > 0 else 0.0
    )

    return {
        "status": "ONLINE",
        "timestamp": time.time(),
        "chapter3_avro_lab": {
            "total_orders": hub.order_system.total_orders,
            "running_avg_price": round(running_avg, 2),
            "success_count": hub.order_system.success_count,
            "retries_count": hub.order_system.glitch_retry_count,
            "dlq_count": len(hub.order_system.dlq_store),
            "dlq_topic": hub.order_system.dlq_topic
        },
        "smart_grid_pipeline": {
            "simulated_day": hub.simulated_day,
            "batch_count": hub.grid_batches,
            "total_telemetry_events": hub.total_grid_events,
            "latest_zones": hub.latest_grid_metrics,
            "db_sink": "PostgreSQL" if hub.grid_processor.db_sink.use_postgres else "SQLite"
        },
        "orchestration": {
            "infrastructure": infra,
            "data_quality_gate": dq
        },
        "streaming_controls": {
            "auto_stream": hub.auto_stream_active,
            "stream_speed_sec": hub.stream_speed
        }
    }


# ---------------------------------------------------------------------------
# REST Endpoints: Interactive Chaos & Order Lab Controls
# ---------------------------------------------------------------------------
@app.post("/api/orders/produce")
async def produce_order(custom: Optional[CustomOrderRequest] = None):
    """Produces and consumes a normal order transaction adhering to order.avsc."""
    hub.order_sequence += 1
    product = (custom.product if custom and custom.product else random.choice(PRODUCTS))
    price = (custom.price if custom and custom.price is not None else round(random.uniform(15.0, 220.0), 2))

    order = {
        "orderId": f"ORD-{hub.order_sequence:04d}",
        "product": product,
        "price": price
    }
    result = hub.order_system.process_order_detailed(order)
    await hub.broadcast({"type": "MANUAL_ORDER", "order": result})
    return {"status": "success", "result": result}


@app.post("/api/orders/inject-glitch")
async def inject_network_glitch():
    """
    Simulates a transient network timeout causing 2 failed attempts,
    exponential backoff, and successful processing on attempt 3.
    """
    hub.order_sequence += 1
    order = {
        "orderId": f"ORD-GLITCH-{hub.order_sequence:04d}",
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(40.0, 180.0), 2)
    }
    # Force 2 transient failures before succeeding on attempt 3
    result = hub.order_system.process_order_detailed(order, force_glitch_attempts=2)
    await hub.broadcast({"type": "MANUAL_ORDER", "order": result})
    return {
        "status": "glitch_simulated",
        "description": "Simulated transient network failure with retry backoff",
        "result": result
    }


@app.post("/api/orders/inject-poison")
async def inject_poison_pill():
    """
    Injects an invalid poison pill message (negative price) that
    violates business integrity, fails all retries, and is quarantined to DLQ.
    """
    hub.order_sequence += 1
    poison_order = {
        "orderId": f"ORD-POISON-{hub.order_sequence:04d}",
        "product": "CorruptedItem",
        "price": -99.99  # Invalid negative price!
    }
    result = hub.order_system.process_order_detailed(poison_order)
    await hub.broadcast({
        "type": "DLQ_ALERT",
        "order": result,
        "dlq_record": result.get("dlq_record")
    })
    return {
        "status": "poison_pill_quarantined",
        "description": "Unrecoverable poison message routed directly to Dead Letter Queue (DLQ)",
        "result": result
    }


@app.get("/api/orders/dlq")
def list_dlq_messages():
    """Returns all messages currently quarantined in the Dead Letter Queue."""
    return {
        "total_quarantined": len(hub.order_system.dlq_store),
        "dlq_topic": hub.order_system.dlq_topic,
        "messages": hub.order_system.dlq_store
    }


@app.post("/api/orders/dlq/replay")
async def replay_dlq_message(req: ReplayDLQRequest):
    """
    Replays a quarantined DLQ message after remediation (e.g., fixing negative price).
    """
    target = None
    for item in hub.order_system.dlq_store:
        if item["dlq_id"] == req.dlq_id:
            target = item
            break

    if not target:
        raise HTTPException(status_code=404, detail=f"DLQ message '{req.dlq_id}' not found")

    repaired_msg = dict(target["failed_message"])
    if req.corrected_price is not None:
        repaired_msg["price"] = abs(req.corrected_price)
    elif float(repaired_msg.get("price", 0)) < 0:
        # Default remediation: make price positive
        repaired_msg["price"] = abs(float(repaired_msg["price"]))

    target["replayed"] = True
    result = hub.order_system.process_order_detailed(repaired_msg, force_glitch_attempts=0)
    await hub.broadcast({"type": "DLQ_REPLAYED", "order": result, "dlq_id": req.dlq_id})

    return {
        "status": "replayed",
        "repaired_message": repaired_msg,
        "result": result
    }


@app.post("/api/stream/toggle")
def toggle_streaming(active: Optional[bool] = None, speed: Optional[float] = None):
    """Toggles or adjusts background streaming rate."""
    if active is not None:
        hub.auto_stream_active = active
    if speed is not None and speed > 0:
        hub.stream_speed = float(speed)

    return {
        "auto_stream_active": hub.auto_stream_active,
        "stream_speed_sec": hub.stream_speed
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.app:app", host="127.0.0.1", port=8000, reload=False)
