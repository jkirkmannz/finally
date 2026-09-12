"""Integration tests for the SSE price streaming endpoint.

`_generate_events` is a `while True` loop that only exits when the client
disconnects. Both `httpx.ASGITransport` and Starlette's `TestClient` fully
await an ASGI call to completion before handing back anything to consume —
neither delivers a disconnect mid-stream, so driving this endpoint through
either one deadlocks (confirmed empirically). Instead, these tests drive
`_generate_events` directly with a minimal fake `Request` whose
`is_disconnected()` we control, and separately exercise the routed endpoint
function (returned by `create_stream_router`) to cover response/header
wiring without consuming its unbounded body.
"""

import asyncio
import json

import pytest
from fastapi.responses import StreamingResponse

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router


class _FakeClient:
    host = "test-client"


class _FakeRequest:
    """Minimal stand-in for fastapi.Request — only what _generate_events uses."""

    def __init__(self) -> None:
        self.client = _FakeClient()
        self._disconnected = False

    async def is_disconnected(self) -> bool:
        return self._disconnected

    def disconnect(self) -> None:
        self._disconnected = True


def _parse_data_event(event: str) -> dict:
    assert event.startswith("data: ")
    assert event.endswith("\n\n")
    return json.loads(event[len("data: ") : -2])


class TestCreateStreamRouter:
    """Tests for the router factory itself (not the streaming body)."""

    def test_builds_independent_routers(self):
        """Each call must return its own APIRouter, not share module state.

        Regression test: create_stream_router() used to decorate onto a
        shared module-level router, so calling it twice would register
        /prices twice on the same object.
        """
        cache = PriceCache()
        router_a = create_stream_router(cache)
        router_b = create_stream_router(cache)

        assert router_a is not router_b
        assert len(router_a.routes) == 1
        assert len(router_b.routes) == 1

    @pytest.mark.asyncio
    async def test_endpoint_returns_streaming_response_with_sse_headers(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        router = create_stream_router(cache)
        endpoint = router.routes[0].endpoint

        response = await endpoint(_FakeRequest())

        assert isinstance(response, StreamingResponse)
        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["connection"] == "keep-alive"
        assert response.headers["x-accel-buffering"] == "no"


@pytest.mark.asyncio
class TestGenerateEvents:
    """Tests for the _generate_events async generator directly."""

    async def test_first_event_is_retry_directive(self):
        cache = PriceCache()
        request = _FakeRequest()
        gen = _generate_events(cache, request, interval=0.01)

        first = await gen.__anext__()
        assert first == "retry: 1000\n\n"

        request.disconnect()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    async def test_emits_seeded_prices(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        cache.update("GOOGL", 175.25)
        request = _FakeRequest()
        gen = _generate_events(cache, request, interval=0.01)

        await gen.__anext__()  # retry directive
        payload = _parse_data_event(await gen.__anext__())

        assert payload["AAPL"]["price"] == 190.50
        assert payload["AAPL"]["direction"] == "flat"
        assert payload["GOOGL"]["price"] == 175.25

        request.disconnect()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    async def test_reflects_subsequent_updates(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        request = _FakeRequest()
        gen = _generate_events(cache, request, interval=0.01)

        await gen.__anext__()  # retry directive
        first_payload = _parse_data_event(await gen.__anext__())
        assert first_payload["AAPL"]["price"] == 190.00

        cache.update("AAPL", 191.00)
        second_payload = _parse_data_event(await gen.__anext__())
        assert second_payload["AAPL"]["price"] == 191.00
        assert second_payload["AAPL"]["direction"] == "up"

        request.disconnect()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    async def test_no_data_event_while_cache_stays_empty(self):
        """With an empty cache, the version never changes, so no data event
        should ever be produced — only the initial retry directive."""
        cache = PriceCache()
        request = _FakeRequest()
        gen = _generate_events(cache, request, interval=0.02)

        first = await gen.__anext__()
        assert first == "retry: 1000\n\n"

        async def disconnect_after_several_ticks() -> None:
            await asyncio.sleep(0.1)  # ~5 ticks at a 0.02s interval
            request.disconnect()

        asyncio.create_task(disconnect_after_several_ticks())

        # If a data event had been produced, anext() would return it instead
        # of the loop eventually hitting the disconnect and raising here.
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    async def test_stops_on_disconnect_mid_stream(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        request = _FakeRequest()
        gen = _generate_events(cache, request, interval=0.01)

        await gen.__anext__()  # retry directive
        await gen.__anext__()  # initial data event

        request.disconnect()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()
