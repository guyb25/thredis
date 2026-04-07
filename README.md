# thredis

Concurrent message processing for Redis Streams.

## Why?

Every Python library used for working with redis streams makes its consumers process messages one at a time.
That means hundreds of idle consumers burning CPU and network just to poll.
Redis Streams supports concurrent processing. No library takes advantage of it. thredis does.

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

## Features

- Process messages concurrently within a single consumer
- Automatic recovery of messages from dead consumers
- Dead letter streams for poison messages
- Built-in backpressure, health checks, and graceful shutdown
- Pydantic v2 serialization, typed handlers, functional middleware

## License

MIT
