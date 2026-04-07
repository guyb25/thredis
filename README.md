# thredis

Async Redis Streams with concurrent processing and per-message ack.

## Why?

Redis Streams support per-message `XACK`, meaning a single consumer can safely process multiple messages concurrently. 
No Python library does this. 
They all process one message at a time per consumer, forcing you to spawn hundreds of consumers for parallelism.
The underlying Redis primitive supports concurrency. The libraries just don't use it.
thredis does.

## Install

```bash
pip install thredis
```

## Quick start

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

app.run()
```

Messages are acked on success. If a handler raises, the message stays in the PEL and gets autoclaimed by another consumer.

## Features

- Process messages concurrently within a single consumer
- Automatic recovery of messages from dead consumers
- Dead letter streams for poison messages
- Built-in backpressure, health checks, and graceful shutdown
- Pydantic v2 serialization, typed handlers, functional middleware

## License

MIT
