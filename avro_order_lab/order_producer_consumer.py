"""
Assignment Chapter 3: Avro Order Processing with Running Average, Retries, and DLQ.

Features:
- Produces and consumes order messages using Avro schema.
- Calculates real-time running average of prices.
- Implements retry logic (up to 3 attempts with backoff) for transient errors.
- Routes unrecoverable / poison messages to a Dead Letter Queue (DLQ) topic.
"""

import sys
import json
import time
import random
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings

logger = settings.get_logger("avro_order_lab")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


class OrderStreamingSystem:
    def __init__(self, dlq_topic="order_dlq"):
        self.dlq_topic = dlq_topic
        self.total_orders = 0
        self.cumulative_price = 0.0
        self.dlq_store = []

    def generate_simulated_order(self, order_num: int) -> dict:
        """Generates order adhering to order.avsc schema."""
        return {
            "orderId": f"ORD-{order_num:04d}",
            "product": random.choice(PRODUCTS),
            "price": round(random.uniform(10.0, 250.0), 2),
        }

    def process_order_message(self, message: dict, max_retries: int = 3) -> bool:
        """
        Consumes an order message with retry logic and DLQ forwarding.
        Simulates transient network/db glitch on random orders.
        """
        for attempt in range(1, max_retries + 1):
            try:
                # Validate schema
                if "orderId" not in message or "price" not in message:
                    raise ValueError("Malformed message: missing required Avro fields")

                price = float(message["price"])
                if price < 0:
                    raise ValueError(f"Poison pill detected: negative price {price}")

                # Simulated transient failure: 10% chance on first attempt
                if attempt == 1 and random.random() < 0.10:
                    raise IOError("Simulated transient connection timeout")

                # Real-time running average calculation
                self.total_orders += 1
                self.cumulative_price += price
                running_avg = self.cumulative_price / self.total_orders

                logger.info(
                    f"Successfully processed {message['orderId']}",
                    extra={"props": {
                        "orderId": message["orderId"],
                        "product": message["product"],
                        "price": price,
                        "running_avg_price": round(running_avg, 2),
                        "total_orders": self.total_orders
                    }}
                )
                return True

            except IOError as transient_err:
                logger.warning(
                    f"Transient error on {message.get('orderId')} (attempt {attempt}/{max_retries}): {transient_err}. Retrying..."
                )
                time.sleep(0.1 * attempt)

            except Exception as unrecoverable_err:
                # Permanent failure -> Route to Dead Letter Queue (DLQ) immediately
                logger.error(
                    f"Permanent error on {message.get('orderId')}: {unrecoverable_err}. Forwarding to DLQ topic '{self.dlq_topic}'."
                )
                self._send_to_dlq(message, reason=str(unrecoverable_err))
                return False

        # Exhausted all retries -> Route to DLQ
        logger.error(
            f"Exhausted all {max_retries} retries for {message.get('orderId')}. Forwarding to DLQ topic '{self.dlq_topic}'."
        )
        self._send_to_dlq(message, reason=f"Max retries ({max_retries}) exceeded")
        return False

    def _send_to_dlq(self, message: dict, reason: str):
        """Dispatches failed message to DLQ queue."""
        dlq_record = {
            "failed_message": message,
            "error_reason": reason,
            "timestamp": time.time()
        }
        self.dlq_store.append(dlq_record)
        logger.info(f"DLQ Record queued: {dlq_record}")


def run_demo(num_orders: int = 15):
    logger.info(f"Starting Chapter 3 Avro Order Lab Demo with {num_orders} orders...")
    system = OrderStreamingSystem()

    for i in range(1, num_orders + 1):
        order = system.generate_simulated_order(i)
        # Inject one intentional poison pill for testing DLQ
        if i == 5:
            order["price"] = -50.0  # Invalid negative price

        system.process_order_message(order)
        time.sleep(0.05)

    final_avg = system.cumulative_price / system.total_orders if system.total_orders else 0.0
    logger.info(
        "Lab Demo Finished",
        extra={"props": {
            "total_processed": system.total_orders,
            "final_running_avg": round(final_avg, 2),
            "dlq_count": len(system.dlq_store)
        }}
    )


if __name__ == "__main__":
    run_demo()
