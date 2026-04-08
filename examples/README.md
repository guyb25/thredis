# Examples

A pipeline of 3 services, each using a different handler mode.

```
publish_orders.py -> [orders stream]
                         |
                   order_service.py        (async)
                         |
             +-----------+-----------+
     [invoices stream]       [notifications stream]
             |                       |
     invoice_service.py      notification_service.py
     (sync, threaded)        (async, threaded)
```

## Running

Start Redis, then in 4 terminals:

```bash
python examples/order_service.py
python examples/invoice_service.py
python examples/notification_service.py
python examples/publish_orders.py
```
