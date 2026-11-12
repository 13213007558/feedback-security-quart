from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from hypercorn.typing import ASGIReceiveEvent
from hypercorn.typing import ASGISendEvent
from hypercorn.typing import HTTPScope
from hypercorn.typing import WebsocketScope
from werkzeug.datastructures import Headers

from quart import Quart
from quart.asgi import _convert_version
from quart.asgi import _handle_exception
from quart.asgi import ASGIHTTPConnection
from quart.asgi import ASGIWebsocketConnection
from quart.signals import got_serving_exception
from quart.utils import encode_headers
from quart.wrappers import Response


@pytest.mark.parametrize(
    "headers, expected", [([(b"host", b"quart")], "quart"), ([], "")]
)
async def test_http_1_0_host_header(headers: list, expected: str) -> None:
    app = Quart(__name__)
    scope: HTTPScope = {
        "type": "http",
        "asgi": {},
        "http_version": "1.0",
        "method": "GET",
        "scheme": "https",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("127.0.0.1", 80),
        "server": None,
        "extensions": {},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIHTTPConnection(app, scope)
    request = connection._create_request_from_scope(lambda: None)  # type: ignore
    assert request.headers["host"] == expected


async def test_http_completion() -> None:
    # Ensure that the connecion callable returns on completion
    app = Quart(__name__)
    scope: HTTPScope = {
        "type": "http",
        "asgi": {},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "extensions": {},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIHTTPConnection(app, scope)

    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait({"type": "http.request", "body": b"", "more_body": False})

    async def receive() -> ASGIReceiveEvent:
        # This will block after returning the first and only entry
        return await queue.get()

    async def send(message: ASGISendEvent) -> None:
        queue.put_nowait({"type": "http.disconnect"})

    # This test fails if a timeout error is raised here
    await asyncio.wait_for(connection(receive, send), timeout=1)


@pytest.mark.parametrize(
    "request_message",
    [
        {"type": "http.request", "body": b"", "more_body": False},
        {"type": "http.request", "more_body": False},
    ],
)
async def test_http_request_without_body(request_message: dict) -> None:
    app = Quart(__name__)

    scope: HTTPScope = {
        "type": "http",
        "asgi": {},
        "http_version": "1.0",
        "method": "GET",
        "scheme": "https",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "extensions": {},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIHTTPConnection(app, scope)
    request = connection._create_request_from_scope(lambda: None)  # type: ignore

    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait(request_message)

    async def receive() -> ASGIReceiveEvent:
        # This will block after returning the first and only entry
        return await queue.get()

    body = None

    async def _handle_request() -> None:
        nonlocal body
        body = await asyncio.wait_for(request.body, timeout=1)
        await queue.put({"type": "http.disconnect"})

    # This test fails with a timeout error if the request body is not received
    # within 1 second
    async with asyncio.TaskGroup() as task_group:
        request_task = task_group.create_task(_handle_request())
        task_group.create_task(
            connection.handle_messages(request, receive, request_task)
        )

    assert body == b""


async def test_websocket_completion() -> None:
    # Ensure that the connecion callable returns on completion
    app = Quart(__name__)
    scope: WebsocketScope = {
        "type": "websocket",
        "asgi": {},
        "http_version": "1.1",
        "scheme": "wss",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "subprotocols": [],
        "extensions": {"websocket.http.response": {}},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIWebsocketConnection(app, scope)

    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait({"type": "websocket.connect"})

    async def receive() -> ASGIReceiveEvent:
        # This will block after returning the first and only entry
        return await queue.get()

    async def send(message: ASGISendEvent) -> None:
        queue.put_nowait({"type": "websocket.disconnect"})

    # This test fails if a timeout error is raised here
    await asyncio.wait_for(connection(receive, send), timeout=1)


def test_http_path_from_absolute_target() -> None:
    app = Quart(__name__)
    scope: HTTPScope = {
        "type": "http",
        "asgi": {},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "http://quart/path",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "extensions": {},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIHTTPConnection(app, scope)
    request = connection._create_request_from_scope(lambda: None)  # type: ignore
    assert request.path == "/path"


@pytest.mark.parametrize(
    "path, expected",
    [("/app", "/ "), ("/", "/ "), ("/app/", "/"), ("/app/2", "/2"), ("/x/app/2", "/ ")],
)
def test_http_path_with_root_path(path: str, expected: str) -> None:
    app = Quart(__name__)
    scope: HTTPScope = {
        "type": "http",
        "asgi": {},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": path,
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "/app",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "extensions": {},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIHTTPConnection(app, scope)
    request = connection._create_request_from_scope(lambda: None)  # type: ignore
    assert request.path == expected


def test_websocket_path_from_absolute_target() -> None:
    app = Quart(__name__)
    scope: WebsocketScope = {
        "type": "websocket",
        "asgi": {},
        "http_version": "1.1",
        "scheme": "wss",
        "path": "ws://quart/path",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "subprotocols": [],
        "extensions": {"websocket.http.response": {}},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIWebsocketConnection(app, scope)
    websocket = connection._create_websocket_from_scope(lambda: None)  # type: ignore
    assert websocket.path == "/path"


@pytest.mark.parametrize(
    "path, expected",
    [("/app", "/ "), ("/", "/ "), ("/app/", "/"), ("/app/2", "/2"), ("/x/app/2", "/ ")],
)
def test_websocket_path_with_root_path(path: str, expected: str) -> None:
    app = Quart(__name__)
    scope: WebsocketScope = {
        "type": "websocket",
        "asgi": {},
        "http_version": "1.1",
        "scheme": "wss",
        "path": path,
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "/app",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "subprotocols": [],
        "extensions": {"websocket.http.response": {}},
        "state": {},  # type: ignore[typeddict-item]
    }
    connection = ASGIWebsocketConnection(app, scope)
    websocket = connection._create_websocket_from_scope(lambda: None)  # type: ignore
    assert websocket.path == expected


@pytest.mark.parametrize(
    "scope, headers, subprotocol, has_headers",
    [
        ({}, Headers(), None, False),
        ({}, Headers(), "abc", False),
        ({"asgi": {"spec_version": "2.1"}}, Headers({"a": "b"}), None, True),
        ({"asgi": {"spec_version": "2.1.1"}}, Headers({"a": "b"}), None, True),
    ],
)
async def test_websocket_accept_connection(
    scope: dict, headers: Headers, subprotocol: str | None, has_headers: bool
) -> None:
    connection = ASGIWebsocketConnection(Quart(__name__), scope)  # type: ignore
    mock_send = AsyncMock()
    await connection.accept_connection(mock_send, headers, subprotocol)

    if has_headers:
        mock_send.assert_called_with(
            {
                "subprotocol": subprotocol,
                "type": "websocket.accept",
                "headers": encode_headers(headers),
            }
        )
    else:
        mock_send.assert_called_with(
            {"headers": [], "subprotocol": subprotocol, "type": "websocket.accept"}
        )


async def test_websocket_accept_connection_warns(
    websocket_scope: WebsocketScope,
) -> None:
    connection = ASGIWebsocketConnection(Quart(__name__), websocket_scope)

    async def mock_send(message: ASGISendEvent) -> None:
        pass

    with pytest.warns(UserWarning):
        await connection.accept_connection(mock_send, Headers({"a": "b"}), None)


def test__convert_version() -> None:
    assert _convert_version("2.1") == [2, 1]


def test_http_asgi_scope_from_request() -> None:
    app = Quart(__name__)
    scope = {
        "headers": [(b"host", b"quart")],
        "http_version": "1.0",
        "method": "GET",
        "scheme": "https",
        "path": "/",
        "query_string": b"",
        "test_result": "PASSED",
    }
    connection = ASGIHTTPConnection(app, scope)  # type: ignore
    request = connection._create_request_from_scope(lambda: None)  # type: ignore
    assert request.scope["test_result"] == "PASSED"  # type: ignore


@pytest.mark.parametrize(
    "debug, testing, raises",
    [
        (True, False, False),
        (True, True, False),
        (False, True, False),
        (False, False, True),
    ],
)
async def test__handle_exception(debug: bool, testing: bool, raises: bool) -> None:
    app = Mock()
    app.config = {}
    app.debug = debug
    app.testing = testing

    if raises:
        with pytest.raises(ValueError):
            await _handle_exception(app, ValueError())
    else:
        await _handle_exception(app, ValueError())


def _streaming_scope() -> HTTPScope:
    return {
        "type": "http",
        "asgi": {"spec_version": "2.1"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/download",
        "raw_path": b"/download",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "extensions": {},
        "state": {},  # type: ignore[typeddict-item]
    }


async def test_streaming_failure_after_first_byte_is_reported() -> None:
    # A generator failure raised after the response started cannot be rewritten
    # into an error response. The bytes already sent must remain, the original
    # error must propagate (never a success status), and the serving signal and
    # error log must record it.
    app = Quart(__name__)
    app.testing = False
    fired: list[BaseException] = []

    def receiver(sender: Any, *, exception: BaseException, **_: Any) -> None:
        fired.append(exception)

    got_serving_exception.connect(receiver, app)

    async def broken_stream() -> AsyncGenerator[bytes, None]:
        yield b"first-chunk"
        raise RuntimeError("body source died")

    @app.get("/download")
    async def download() -> Response:
        return app.response_class(broken_stream(), status=200)

    sent: list[ASGISendEvent] = []
    inbound: asyncio.Queue = asyncio.Queue()
    inbound.put_nowait({"type": "http.request", "body": b"", "more_body": False})
    request_consumed = asyncio.Event()

    async def receive() -> ASGIReceiveEvent:
        # Hand over the request body, then block like a still-connected client
        # so a sending failure propagates instead of being cancelled by a
        # disconnect message.
        if not request_consumed.is_set():
            message = await inbound.get()
            request_consumed.set()
            return message
        await asyncio.Event().wait()
        raise RuntimeError("unreachable")  # pragma: no cover

    async def send(message: ASGISendEvent) -> None:
        sent.append(message)

    connection = ASGIHTTPConnection(app, _streaming_scope())
    with pytest.raises(BaseExceptionGroup) as exc_info:
        await connection(receive, send)

    leaves = exc_info.value.exceptions
    assert any(isinstance(exc, RuntimeError) for exc in leaves)
    assert fired and isinstance(fired[0], RuntimeError)
    # Status 200 and the first chunk were already committed before the failure.
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 200  # type: ignore[index]
    assert b"first-chunk" in b"".join(
        event.get("body", b"")
        for event in sent
        if event["type"] == "http.response.body"
    )
    # No clean terminal frame is sent for an aborted response.
    assert not any(
        event["type"] == "http.response.body" and not event.get("more_body", True)
        for event in sent
    )
    got_serving_exception.disconnect(receiver, app)


async def test_streaming_failure_before_first_byte_skips_sending_report() -> None:
    # A teardown failure raised while the request context exits happens before
    # the first byte is sent. It must surface unchanged and must NOT be routed
    # through the post-send reporting path (which only applies once headers are
    # committed).
    app = Quart(__name__)
    app.testing = False
    reported: list[BaseException] = []

    def receiver(sender: Any, *, exception: BaseException, **_: Any) -> None:
        reported.append(exception)

    got_serving_exception.connect(receiver, app)

    @app.teardown_request
    async def fail_teardown(exc: BaseException | None) -> None:
        raise IOError("cleanup failed")

    @app.get("/download")
    async def download() -> Response:
        async def stream() -> AsyncGenerator[bytes, None]:
            yield b"never-sent"

        return app.response_class(stream(), status=200)

    sent: list[ASGISendEvent] = []
    inbound: asyncio.Queue = asyncio.Queue()
    inbound.put_nowait({"type": "http.request", "body": b"", "more_body": False})
    request_consumed = asyncio.Event()

    async def receive() -> ASGIReceiveEvent:
        if not request_consumed.is_set():
            message = await inbound.get()
            request_consumed.set()
            return message
        await asyncio.Event().wait()
        raise RuntimeError("unreachable")  # pragma: no cover

    async def send(message: ASGISendEvent) -> None:
        sent.append(message)

    connection = ASGIHTTPConnection(app, _streaming_scope())
    with pytest.raises(BaseExceptionGroup):
        await connection(receive, send)

    assert reported == []
    assert sent == []  # nothing committed: teardown failure is not hidden as 200 OK
    got_serving_exception.disconnect(receiver, app)
