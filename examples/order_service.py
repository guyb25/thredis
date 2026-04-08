"""Order service - async handlers on the event loop.

Receives orders, validates them, publishes invoices downstream.
Propagates correlation_id through the pipeline via headers.

    python examples/order_service.py
"""

import logging
import uuid

from models import Invoice, Notification, Order

from thredis import Thredis, StreamMessage, publish
from thredis.middlewares import trace

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

app = Thredis(redis_url="redis://localhost:6379", health_port=5006, middlewares=[trace()])


@app.subscriber(stream="orders", group="order-service", concurrency=10)
async def process_order(msg: StreamMessage[Order]):
    cid = str(uuid.uuid4())[:8]
    headers = {"correlation_id": cid, "source": "order-service"}

    logging.info(f"[cid={cid}] Processing order {msg.body.order_id} from {msg.body.customer}")

    await publish(
        "invoices",
        Invoice(
            order_id=msg.body.order_id,
            customer=msg.body.customer,
            total=msg.body.amount * 1.1,
            pdf_url=f"https://invoices.example.com/{msg.body.order_id}.pdf",
        ),
        headers=headers,
    )

    await publish(
        "notifications",
        Notification(
            recipient=msg.body.customer,
            subject=f"Order {msg.body.order_id} confirmed",
            body=f"Your order of ${msg.body.amount:.2f} has been confirmed.",
        ),
        headers=headers,
    )

    logging.info(f"[cid={cid}] Order {msg.body.order_id} done")


if __name__ == "__main__":
    app.run()
