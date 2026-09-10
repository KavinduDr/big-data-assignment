"""
Unit and Integration Tests for FastAPI Web Operations Center.

Covers:
- Root HTML serving
- System status health endpoint
- Manual order generation
- Transient network glitch injection & retry verification
- Poison pill injection & DLQ quarantine
- DLQ listing and message replay / re-drive
- Stream toggling and rate control
- Real-time WebSocket connection handshake and INIT_STATE delivery
"""

import sys
import json
import unittest
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi.testclient import TestClient
from web.app import app, hub


class TestWebServerAndEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_root_serves_html(self):
        """Verifies root endpoint serves the operations dashboard."""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Streaming Operations Center", response.text)

    def test_status_endpoint(self):
        """Verifies /api/status returns proper architecture metrics."""
        response = self.client.get("/api/status")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "ONLINE")
        self.assertIn("chapter3_avro_lab", data)
        self.assertIn("smart_grid_pipeline", data)
        self.assertIn("running_avg_price", data["chapter3_avro_lab"])

    def test_produce_valid_order(self):
        """Tests standard order production."""
        response = self.client.post("/api/orders/produce", json={"product": "Item3", "price": 85.50})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["result"]["product"], "Item3")
        self.assertEqual(data["result"]["price"], 85.50)
        self.assertIn(data["result"]["status"], ["SUCCESS", "RECOVERED_AFTER_RETRY"])
        self.assertGreater(data["result"]["running_avg"], 0)

    def test_inject_transient_glitch(self):
        """Tests transient network failure retry backoff behavior."""
        response = self.client.post("/api/orders/inject-glitch")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "glitch_simulated")
        # Ensure attempts reached 3 (2 retries then success)
        self.assertEqual(data["result"]["attempts"], 3)
        self.assertEqual(data["result"]["status"], "RECOVERED_AFTER_RETRY")

    def test_inject_poison_pill_and_dlq_quarantine(self):
        """Tests poison pill injection (-$99.99) and routing to DLQ."""
        initial_dlq_count = len(hub.order_system.dlq_store)

        response = self.client.post("/api/orders/inject-poison")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "poison_pill_quarantined")
        self.assertEqual(data["result"]["status"], "DLQ_QUARANTINED")
        self.assertIn("dlq_record", data["result"])

        # DLQ count should increase by 1
        self.assertEqual(len(hub.order_system.dlq_store), initial_dlq_count + 1)

    def test_list_dlq_messages(self):
        """Verifies GET /api/orders/dlq returns quarantined items."""
        response = self.client.get("/api/orders/dlq")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("messages", data)
        self.assertIsInstance(data["messages"], list)

    def test_replay_dlq_message(self):
        """Tests re-driving a message from DLQ after remediating price."""
        # Inject poison pill first to ensure at least one item
        res = self.client.post("/api/orders/inject-poison")
        dlq_id = res.json()["result"]["dlq_record"]["dlq_id"]

        replay_res = self.client.post("/api/orders/dlq/replay", json={"dlq_id": dlq_id, "corrected_price": 45.0})
        self.assertEqual(replay_res.status_code, 200)
        data = replay_res.json()
        self.assertEqual(data["status"], "replayed")
        self.assertEqual(data["result"]["status"], "SUCCESS")
        self.assertEqual(data["repaired_message"]["price"], 45.0)

    def test_stream_toggle(self):
        """Tests pausing and resuming the background stream."""
        response = self.client.post("/api/stream/toggle?active=false&speed=2.5")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertFalse(data["auto_stream_active"])
        self.assertEqual(data["stream_speed_sec"], 2.5)

        # Restore
        self.client.post("/api/stream/toggle?active=true&speed=1.0")

    def test_websocket_connection(self):
        """Verifies WebSocket handshake and delivery of initial state."""
        with self.client.websocket_connect("/ws/stream") as websocket:
            data = websocket.receive_text()
            msg = json.loads(data)
            self.assertEqual(msg["type"], "INIT_STATE")
            self.assertIn("order_stats", msg)
            self.assertIn("grid", msg)


if __name__ == "__main__":
    unittest.main()
