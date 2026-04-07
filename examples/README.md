# Examples

Three services forming a pipeline, each demonstrating a different handler mode.
Correlation IDs flow through the entire pipeline via headers.

```
publish_orders.py -> [orders stream]
                         |
                   order_service.py        (async, concurrency=10)
                         |
             +-----------+-----------+
     [invoices stream]       [notifications stream]
             |                       |
     invoice_service.py      notification_service.py
     (sync, threaded=True)   (async, threaded=True)
```

## Running

Start Redis, then open 4 terminals:

```bash
# Terminal 1 - order service (async handlers on event loop)
python examples/order_service.py

# Terminal 2 - invoice service (sync handler, threaded=True for CPU-heavy PDF work)
python examples/invoice_service.py

# Terminal 3 - notification service (async handler, threaded=True for CPU+I/O mix)
python examples/notification_service.py

# Terminal 4 - publish sample orders
python examples/publish_orders.py
```

## What each service demonstrates

| Service | Handler | Mode | Features shown |
|---------|---------|------|----------------|
| order_service | `async def` | Event loop | `StreamMessage[T]`, `publish()` from handler, correlation ID generation, headers propagation, trace middleware |
| invoice_service | `def` (sync) | `threaded=True` | Sync handler with explicit threading, `time.sleep()` without blocking event loop, reading headers in middleware |
| notification_service | `async def` | `threaded=True` | Async handler in own thread + event loop, mixed CPU (`time.sleep`) + I/O (`await`), headers tracing |

## Key patterns shown

- **Headers for tracing**: `order_service` generates a `correlation_id` and passes it via `headers` to downstream services
- **Middleware**: All services use a `trace_middleware` that logs `[cid=xxx]` from `msg.headers`
- **`publish()` without app reference**: `order_service` uses the context-based `publish()` inside handlers
- **`add_middleware()`**: All services add middleware via the public API
- **`StreamMessage[T]`**: All handlers receive typed messages with `.body`, `.headers`, `.msg_id`, `.stream`
