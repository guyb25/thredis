from pydantic import BaseModel

from thredis import Thredis


class Notification(BaseModel):
    user_id: str
    text: str


async def _start_app_consumers(app: Thredis, redis_url: str) -> None:
    """Helper: start subscribers for an app without blocking on signals."""
    await app._ensure_publisher()
    for sub in app._subscribers:
        sub.middlewares = app._middlewares + sub.middlewares
        sub.app = app
        from thredis.middleware import build_chain
        sub._chain = build_chain(sub.handler, sub.middlewares, threaded=sub.threaded)
        await sub.start(redis_url)
