from __future__ import annotations

import asyncio

import pytest
from hypercorn.typing import ASGIReceiveEvent
from hypercorn.typing import ASGISendEvent
from hypercorn.typing import WebsocketScope
from werkzeug.exceptions import InternalServerError

from quart import current_app
from quart import Quart
from quart import session
from quart import websocket
from quart.sessions import SessionSaveError
from quart.testing import WebsocketResponseError


def _websocket_scope(spec_version: str = "2.1") -> WebsocketScope:
    return {
        "type": "websocket",
        "asgi": {"spec_version": spec_version},
        "http_version": "1.1",
        "scheme": "wss",
        "path": "/ws",
        "raw_path": b"/ws",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "subprotocols": [],
        "extensions": {"websocket.http.response": {}},
        "state": {},  # type: ignore[typeddict-item]
    }


async def _run_websocket(app: Quart, scope: WebsocketScope) -> list[ASGISendEvent]:
    """Drive the app as an ASGI server, returning the events sent."""
    events: list[ASGISendEvent] = []
    inbound: asyncio.Queue = asyncio.Queue()
    inbound.put_nowait({"type": "websocket.connect"})

    async def receive() -> ASGIReceiveEvent:
        return await inbound.get()

    async def send(message: ASGISendEvent) -> None:
        events.append(message)
        if message["type"] in {"websocket.accept", "websocket.close"}:
            inbound.put_nowait({"type": "websocket.disconnect"})
        elif message["type"] == "websocket.http.response.body" and not message.get(
            "more_body", False
        ):
            inbound.put_nowait({"type": "websocket.disconnect"})

    await asyncio.wait_for(app.asgi_app(scope, receive, send), timeout=1)
    return events


async def test_login_before_accept_persists_session_via_accept_headers() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"

    @app.websocket("/ws")
    async def ws() -> None:
        session["user_id"] = "bob"
        await websocket.accept()

    @app.get("/")
    async def identity() -> str:
        return session.get("user_id", "anonymous")

    events = await _run_websocket(app, _websocket_scope())

    accept_event = next(
        event for event in events if event["type"] == "websocket.accept"
    )
    accept_headers = dict(accept_event["headers"])  # type: ignore[attr-defined]
    assert b"set-cookie" in accept_headers
    cookie = accept_headers[b"set-cookie"].decode().split(";", 1)[0]

    # The next request carrying the new cookie must see the identity
    response = await app.test_client().get("/", headers={"Cookie": cookie})
    assert (await response.get_data()) == b"bob"


async def test_privilege_escalation_after_accept_cannot_be_persisted() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"
    handled: list[BaseException | None] = []

    @app.errorhandler(500)
    async def handle_500(error: InternalServerError) -> tuple[str, int]:
        handled.append(error.original_exception)
        return "", 500

    @app.websocket("/ws")
    async def ws() -> None:
        await websocket.accept()
        # The capability contract: after acceptance there is no
        # response to carry session changes to the client.
        assert not current_app.session_interface.can_save_session(None)
        session["role"] = "admin"  # This escalation cannot be persisted

    @app.get("/")
    async def identity() -> str:
        return session.get("role", "user")

    client = app.test_client()
    async with client.websocket("/ws"):
        pass

    # The unfulfillable change surfaces as a handleable error rather
    # than being silently dropped.
    assert len(handled) == 1
    assert isinstance(handled[0], SessionSaveError)

    # The escalation must not be visible on the next request
    response = await client.get("/")
    assert (await response.get_data()) == b"user"


async def test_rejected_handshake_persists_session_on_response() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"

    @app.websocket("/ws")
    async def ws() -> tuple[str, int]:
        session["user_id"] = "bob"
        return "denied", 403

    @app.get("/")
    async def identity() -> str:
        return session.get("user_id", "anonymous")

    client = app.test_client()
    async with client.websocket("/ws") as connection:
        with pytest.raises(WebsocketResponseError) as exc_info:
            await connection.receive()

    response = exc_info.value.response
    assert response.status_code == 403
    cookie = response.headers["Set-Cookie"].split(";", 1)[0]

    # The next request carrying the cookie must see the identity
    next_response = await client.get("/", headers={"Cookie": cookie})
    assert (await next_response.get_data()) == b"bob"


async def test_modified_session_accept_without_header_support_fails() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"

    @app.errorhandler(SessionSaveError)
    async def handle_save_error(error: SessionSaveError) -> tuple[str, int]:
        return "cannot persist session", 500

    @app.websocket("/ws")
    async def ws() -> None:
        session["user_id"] = "bob"
        await websocket.accept()

    # ASGI spec version 2.0 cannot carry headers on the accept message
    events = await _run_websocket(app, _websocket_scope(spec_version="2.0"))

    assert not any(event["type"] == "websocket.accept" for event in events)
    start_event = next(
        event for event in events if event["type"] == "websocket.http.response.start"
    )
    assert start_event["status"] == 500  # type: ignore[attr-defined]
