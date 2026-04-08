"""Invoice service - threaded sync handler for CPU-heavy PDF generation.

Receives invoices, generates PDFs (CPU-bound), saves them.

    python examples/invoice_service.py
"""

import logging
import time

from models import Invoice

from thredis import Thredis, StreamMessage
from thredis.middlewares import trace

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

app = Thredis(redis_url="redis://localhost:6379", middlewares=[trace()])


@app.subscriber(stream="invoices", group="invoice-service", concurrency=4, threaded=True)
def generate_pdf(msg: StreamMessage[Invoice]):
    logging.info(f"Generating PDF for invoice {msg.body.order_id}...")

    time.sleep(2)

    logging.info(
        f"PDF saved to {msg.body.pdf_url} "
        f"(customer={msg.body.customer}, total=${msg.body.total:.2f})"
    )


if __name__ == "__main__":
    app.run()
