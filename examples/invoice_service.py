"""Invoice service - threaded sync handler for CPU-heavy PDF generation.

Receives invoices, generates PDFs (CPU-bound), saves them.
Reads correlation_id from headers for tracing.

    python examples/invoice_service.py
"""

import logging
import time

from models import Invoice

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


# Sync handler - automatically runs in a thread
@app.subscriber(stream="invoices", group="invoice-service", concurrency=4, threaded=True)
def generate_pdf(msg: StreamMessage[Invoice]):
    logging.info(f"Generating PDF for invoice {msg.body.order_id}...")

    # Simulate CPU-heavy PDF generation
    time.sleep(2)

    logging.info(
        f"PDF saved to {msg.body.pdf_url} "
        f"(customer={msg.body.customer}, total=${msg.body.total:.2f})"
    )


if __name__ == "__main__":
    app.run()
