"""Minimal HTTP health check server using raw asyncio."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any, Callable, Awaitable, Optional

if TYPE_CHECKING:
    pass

logger = logging.getLogger("thredis")

GetStatusFunc = Callable[[], Awaitable[dict[str, Any]]]


class HealthServer:
    """Tiny HTTP server that responds to GET /health with app status."""

    def __init__(self, port: int, get_status: GetStatusFunc) -> None:
        self._port = port
        self._get_status = get_status
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection, host="0.0.0.0", port=self._port
        )
        logger.info(f"Health check listening on port {self._port}")

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)

            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if line == b"\r\n" or line == b"\n" or line == b"":
                    break

            status = await self._get_status()
            is_healthy = status.get("healthy", False)

            body = json.dumps(status).encode()
            status_code = 200 if is_healthy else 503
            status_text = "OK" if is_healthy else "Service Unavailable"

            response = (
                f"HTTP/1.1 {status_code} {status_text}\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            ).encode() + body

            writer.write(response)
            await writer.drain()
        except Exception:
            pass  # Don't crash on bad requests
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
