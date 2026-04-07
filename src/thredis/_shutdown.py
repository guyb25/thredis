from __future__ import annotations

import asyncio
import logging
import signal

logger = logging.getLogger("thredis")

_force_exit = False


def install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _handle(sig: signal.Signals) -> None:
        global _force_exit
        if _force_exit:
            logger.warning("Force exit requested, shutting down immediately")
            raise SystemExit(1)

        logger.info(f"Received {sig.name}, shutting down gracefully...")
        shutdown_event.set()
        _force_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle, sig)
