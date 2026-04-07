"""Publish sample orders to test the pipeline.

    python examples/publish_orders.py
"""

import asyncio
import logging

from models import Order

from thredis import Thredis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

app = Thredis(redis_url="redis://localhost:6379")

SAMPLE_ORDERS = [
    Order(order_id="ORD-001", customer="alice@example.com", amount=99.99, items=["widget", "gadget"]),
    Order(order_id="ORD-002", customer="bob@example.com", amount=249.50, items=["laptop-stand"]),
    Order(order_id="ORD-003", customer="carol@example.com", amount=15.00, items=["usb-cable", "adapter"]),
    Order(order_id="ORD-004", customer="dave@example.com", amount=599.00, items=["monitor"]),
    Order(order_id="ORD-005", customer="eve@example.com", amount=42.00, items=["keyboard", "mouse"]),
]


async def main():
    for order in SAMPLE_ORDERS:
        msg_id = await app.publish("orders", order)
        logging.info(f"Published {order.order_id} -> {msg_id}")

    logging.info(f"Published {len(SAMPLE_ORDERS)} orders")


if __name__ == "__main__":
    asyncio.run(main())
