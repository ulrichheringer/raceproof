from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any


@asynccontextmanager
async def asgi_client(app: Any, *, base_url: str = "http://testserver") -> AsyncIterator[Any]:
    """Create an httpx client for ASGI. Manage application lifespan separately."""
    try:
        import httpx
    except ImportError as error:
        raise ImportError("Install raceproof[http] to use asgi_client") from error
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=base_url
    ) as client:
        yield client
