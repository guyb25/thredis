"""Notification service - threaded async handler for CPU + I/O mix.

Receives notifications, renders email templates (CPU), sends via SMTP (I/O).
Uses threaded=True with an async handler to get its own event loop in a thread.
Reads correlation_id from headers for tracing.

    python examples/notification_service.py
"""

import asyncio
import logging
import time

from models import Notification

from thredis import Thredis, StreamMessage, CallNext

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

app = Thredis(redis_url="redis://localhost:6379")


async def trace_middleware(msg: StreamMessage, call_next: CallNext) -> None:
    cid = msg.headers.get("correlation_id", "unknown")
    source = msg.headers.get("source", "unknown")
    logging.info(f"[cid={cid}] Received from {source}")
    await call_next(msg)
    logging.info(f"[cid={cid}] Complete")


app.add_middleware(trace_middleware)


@app.subscriber(
    stream="notifications",
    group="notification-service",
    concurrency=8,
    threaded=True,
)
async def send_notification(msg: StreamMessage[Notification]):
    logging.info(f"Rendering email for {msg.body.recipient}...")

    # CPU-bound: render email template
    time.sleep(0.5)

    # I/O-bound: send via SMTP
    await asyncio.sleep(0.2)

    logging.info(f"Email sent to {msg.body.recipient}: {msg.body.subject}")


if __name__ == "__main__":
    app.run()
