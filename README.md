# thredis

Async Redis Streams with concurrent processing and per-message acknowledgment.

## Why?

Redis Streams supports per-message `XACK`, which means a single consumer can safely process multiple messages concurrently. No existing Python library takes advantage of this - they all process sequentially (one message at a time per consumer), forcing you to spawn hundreds of consumers for parallelism.

thredis fixes that.

## Install

```bash
pip install thredis
```

## Quick Start

```python
from pydantic import BaseModel
from thredis import Thredis, StreamMessage

app = Thredis(redis_url="redis://localhost:6379")

class Order(BaseModel):
    order_id: str
    amount: float

@app.subscriber(stream="orders", group="order-service", concurrency=10)
async def handle_order(msg: StreamMessage[Order]):
    print(f"Processing {msg.body.order_id}")
    # acked automatically on success
    # stays in PEL on failure -> autoclaimed later

app.run()
```

## Features

### Concurrent Processing

Process up to N messages concurrently within a single consumer. Backpressure is handled automatically - when all slots are full, the consumer stops reading until a slot frees up.

```python
@app.subscriber(stream="orders", group="svc", concurrency=20)
async def handle(msg: StreamMessage[Order]):
    await process(msg.body)  # up to 20 running in parallel
```

### Per-Message Acknowledgment

Each message is acked individually on success. If a handler fails, the message stays in the Pending Entries List (PEL) and gets autoclaimed by another consumer after a configurable timeout.

```python
@app.subscriber(stream="orders", group="svc", claim_idle_after=30_000)
async def handle(msg: StreamMessage[Order]):
    if bad_data(msg.body):
        raise ValueError("bad data")  # NOT acked - autoclaimed later
    await process(msg.body)            # acked on success
```

### Dead Letter Stream

Messages that fail repeatedly can be moved to a dead letter stream after exceeding `max_retries`. Original data and headers are preserved, with metadata about the failure.

```python
@app.subscriber(
    stream="orders",
    group="svc",
    claim_idle_after=30_000,
    max_retries=3,
    dead_letter_stream="orders-dead",
)
async def handle(msg: StreamMessage[Order]):
    ...  # after 3 failed delivery attempts, moved to "orders-dead"
```

### StreamMessage\[T\]

Handlers receive a typed `StreamMessage[T]` with the deserialized body, headers, and stream metadata.

```python
@app.subscriber(stream="orders", group="svc")
async def handle(msg: StreamMessage[Order]):
    msg.body       # Order (typed)
    msg.headers    # dict[str, str]
    msg.msg_id     # "1775294463443-0"
    msg.stream     # "orders"
```

### Headers

Pass metadata (correlation IDs, trace context, routing info) alongside the message body.

```python
await app.publish(
    "orders",
    Order(order_id="123", amount=99.99),
    headers={"correlation_id": "abc-123", "source": "api-gateway"},
)

@app.subscriber(stream="orders", group="svc")
async def handle(msg: StreamMessage[Order]):
    cid = msg.headers.get("correlation_id")
```

### Subscriber Groups

Group subscribers with shared configuration and middleware. Like route groups in web frameworks.

```python
from thredis import SubscriberGroup

orders = SubscriberGroup(
    group="order-service",
    concurrency=10,
    claim_idle_after=30_000,
    max_retries=3,
    dead_letter_stream="dead-letters",
)

@orders.subscriber(stream="orders")
async def handle_order(msg: StreamMessage[Order]): ...

@orders.subscriber(stream="refunds")
async def handle_refund(msg: StreamMessage[Refund]): ...

# Override group defaults per subscriber
@orders.subscriber(stream="bulk-imports", concurrency=2)
async def handle_bulk(msg: StreamMessage[BulkImport]): ...

app.include_group(orders)
```

Groups can have their own middleware that runs after app-level middleware:

```python
orders = SubscriberGroup(group="order-service")
orders.add_middleware(order_trace_middleware)
```

### Threaded Handlers

Sync handlers must set `threaded=True` explicitly. Async handlers can opt into their own thread (with a fresh event loop) for mixed CPU + I/O work. For threaded subscribers, the entire middleware chain runs in the same thread as the handler.

```python
# Sync handler - must be explicit about threading
@app.subscriber(stream="reports", group="svc", concurrency=4, threaded=True)
def generate_report(msg: StreamMessage[ReportRequest]):
    build_pdf(msg.body)  # CPU-heavy, doesn't block the event loop

# Async handler with its own thread + event loop
@app.subscriber(stream="emails", group="svc", concurrency=8, threaded=True)
async def send_email(msg: StreamMessage[EmailRequest]):
    time.sleep(0.5)           # CPU: render template
    await smtp.send(msg.body) # I/O: send email
```

### Middleware

Functional middleware wrapping the handler chain.

```python
from thredis import StreamMessage, CallNext

async def trace_middleware(msg: StreamMessage, call_next: CallNext) -> None:
    cid = msg.headers.get("correlation_id", "?")
    print(f"[{cid}] Start")
    await call_next(msg)
    print(f"[{cid}] Done")

app = Thredis(middlewares=[trace_middleware])
# or
app.add_middleware(trace_middleware)
```

### Publish from Handlers

Publish from inside a handler without needing a reference to the app. Thread-safe.

```python
from thredis import publish

@app.subscriber(stream="orders", group="svc")
async def handle(msg: StreamMessage[Order]):
    await publish("notifications", Notification(text="Order received"))
```

### Consumer-Side Stream Trimming

Streams are trimmed by the consumer, not the publisher. Uses approximate `XTRIM` periodically.

```python
@app.subscriber(stream="orders", group="svc", maxlen=3000)
async def handle(msg: StreamMessage[Order]): ...
```

### Graceful Shutdown

On `SIGINT`/`SIGTERM`, consumers stop reading new messages, wait for in-flight handlers to finish (up to a configurable timeout), then clean up. Unfinished messages stay in the PEL for autoclaim by other consumers.

### Dead Consumer Cleanup

Consumers that have been idle too long and have no pending messages are automatically removed from the consumer group.

## Configuration

```python
app = Thredis(
    redis_url="redis://localhost:6379",
    middlewares=[trace_middleware],
    shutdown_timeout=30.0,
    log_level="INFO",
)

@app.subscriber(
    stream="orders",
    group="order-service",
    concurrency=10,               # max concurrent handlers
    batch_size=10,                # messages per XREADGROUP call
    claim_idle_after=30_000,      # autoclaim after 30s idle (ms)
    max_retries=3,                # dead letter after 3 failures
    dead_letter_stream="dead",    # where poison messages go
    block_timeout=2000,           # XREADGROUP block timeout (ms)
    threaded=False,               # True for sync or CPU-heavy handlers
    maxlen=3000,                  # consumer-side stream trimming
)
```

## How It Works

```
XREADGROUP (batch)
    |
    +-- msg 1 --> semaphore.acquire() --> asyncio.Task --> handler --> XACK
    +-- msg 2 --> semaphore.acquire() --> asyncio.Task --> handler --> XACK
    +-- msg 3 --> semaphore.acquire() --> asyncio.Task --> handler --> (fail -> PEL)
    +-- msg 4 --> semaphore.acquire() --> (blocked until a slot frees up)
                          |
                    backpressure
```

- The consumer reads batches via `XREADGROUP` with `COUNT=batch_size`
- Each message gets a semaphore slot and its own `asyncio.Task`
- When all slots are full, the read loop blocks (backpressure)
- On success: `XACK` per message
- On failure: message stays in PEL, autoclaimed via `XAUTOCLAIM` after `claim_idle_after` ms
- After `max_retries` failed deliveries: moved to dead letter stream or discarded
- Each consumer gets a dedicated Redis connection (blocking `XREADGROUP` won't exhaust the pool)

## License

MIT
