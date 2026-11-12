from __future__ import annotations

import asyncio
import threading
from typing import cast
from unittest.mock import Mock

import pytest
from hypercorn.typing import ASGIReceiveEvent
from hypercorn.typing import ASGISendEvent
from hypercorn.typing import HTTPScope
from werkzeug.datastructures import Headers
from werkzeug.exceptions import BadRequest

from quart.app import Quart
from quart.asgi import ASGIHTTPConnection
from quart.ctx import after_this_request
from quart.ctx import AppContext
from quart.ctx import ContextState
from quart.ctx import copy_current_app_context
from quart.ctx import copy_current_request_context
from quart.ctx import copy_current_websocket_context
from quart.ctx import has_app_context
from quart.ctx import has_request_context
from quart.globals import auth_state
from quart.globals import current_app
from quart.globals import request
from quart.globals import websocket
from quart.helpers import stream_with_context
from quart.routing import QuartRule
from quart.testing import make_test_headers_path_and_query_string
from quart.testing import no_op_push
from quart.wrappers import Request


async def test_request_context_match(http_scope: HTTPScope) -> None:
    app = Quart(__name__)
    url_adapter = Mock()
    rule = QuartRule("/", methods={"GET"}, endpoint="index")
    url_adapter.match.return_value = (rule, {"arg": "value"})
    app.create_url_adapter = lambda *_: url_adapter  # type: ignore
    request = Request(
        "GET",
        "http",
        "/",
        b"",
        Headers([("host", "quart.com")]),
        "",
        "1.1",
        http_scope,
        send_push_promise=no_op_push,
    )
    async with AppContext(app, request=request):
        assert request.url_rule == rule
        assert request.view_args == {"arg": "value"}


async def test_bad_request_if_websocket_route(http_scope: HTTPScope) -> None:
    app = Quart(__name__)
    url_adapter = Mock()
    url_adapter.match.side_effect = BadRequest()
    app.create_url_adapter = lambda *_: url_adapter  # type: ignore
    request = Request(
        "GET",
        "http",
        "/",
        b"",
        Headers([("host", "quart.com")]),
        "",
        "1.1",
        http_scope,
        send_push_promise=no_op_push,
    )
    async with AppContext(app, request=request):
        assert isinstance(request.routing_exception, BadRequest)


async def test_after_this_request(http_scope: HTTPScope) -> None:
    app = Quart(__name__)
    headers, path, query_string = make_test_headers_path_and_query_string(app, "/")
    async with AppContext(
        Quart(__name__),
        request=Request(
            "GET",
            "http",
            path,
            query_string,
            headers,
            "",
            "1.1",
            http_scope,
            send_push_promise=no_op_push,
        ),
    ) as context:
        after_this_request(lambda: "hello")  # type: ignore
        assert context._after_request_functions[0]() == "hello"  # type: ignore


async def test_has_request_context(http_scope: HTTPScope) -> None:
    app = Quart(__name__)
    headers, path, query_string = make_test_headers_path_and_query_string(app, "/")
    request = Request(
        "GET",
        "http",
        path,
        query_string,
        headers,
        "",
        "1.1",
        http_scope,
        send_push_promise=no_op_push,
    )
    async with AppContext(Quart(__name__), request=request):
        assert has_request_context() is True
        assert has_app_context() is True
    assert has_request_context() is False
    assert has_app_context() is False


async def test_has_app_context() -> None:
    async with AppContext(Quart(__name__)):
        assert has_app_context() is True
    assert has_app_context() is False


async def test_copy_current_app_context() -> None:
    app = Quart(__name__)

    @app.route("/")
    async def index() -> str:
        current_app.foo = "bar"  # type: ignore

        @copy_current_app_context
        async def within_context() -> None:
            assert current_app.foo == "bar"  # type: ignore

        await asyncio.ensure_future(within_context())
        return ""

    test_client = app.test_client()
    response = await test_client.get("/")
    assert response.status_code == 200


def test_copy_current_app_context_error() -> None:
    with pytest.raises(RuntimeError):
        copy_current_app_context(lambda: None)()


async def test_copy_current_request_context() -> None:
    app = Quart(__name__)

    @app.route("/")
    async def index() -> str:
        @copy_current_request_context
        async def within_context() -> None:
            assert request.path == "/"

        await asyncio.ensure_future(within_context())
        return ""

    test_client = app.test_client()
    response = await test_client.get("/")
    assert response.status_code == 200


def test_copy_current_request_context_error() -> None:
    with pytest.raises(RuntimeError):
        copy_current_request_context(lambda: None)()


async def test_works_without_copy_current_request_context() -> None:
    app = Quart(__name__)

    @app.route("/")
    async def index() -> str:
        async def within_context() -> None:
            assert request.path == "/"

        await asyncio.ensure_future(within_context())
        return ""

    test_client = app.test_client()
    response = await test_client.get("/")
    assert response.status_code == 200


async def test_copy_current_websocket_context() -> None:
    app = Quart(__name__)

    @app.websocket("/")
    async def index() -> None:
        @copy_current_websocket_context
        async def within_context() -> str:
            return websocket.path

        data = await asyncio.ensure_future(within_context())
        await websocket.send(data.encode())

    test_client = app.test_client()
    async with test_client.websocket("/") as test_websocket:
        data = await test_websocket.receive()
    assert cast(bytes, data) == b"/"


def test_copy_current_websocket_context_error() -> None:
    with pytest.raises(RuntimeError):
        copy_current_websocket_context(lambda: None)()


def test_context_state_api() -> None:
    state = ContextState()
    assert state.valid is True
    state.user = "alice"
    assert state.user == "alice"
    assert "user" in state
    assert state.get("user") == "alice"
    assert state.get("missing") is None
    del state.user
    assert "user" not in state
    with pytest.raises(AttributeError):
        state.user
    state.invalidate()
    assert state.valid is False
    with pytest.raises(RuntimeError):
        state.user
    with pytest.raises(RuntimeError):
        state.other = "value"
    with pytest.raises(RuntimeError):
        "user" in state


async def test_context_state_app_context() -> None:
    app = Quart(__name__)
    async with app.app_context() as ctx:
        assert ctx.state.valid is True
        ctx.state.user = "alice"
        assert auth_state.user == "alice"
    assert ctx.state.valid is False
    with pytest.raises(RuntimeError):
        ctx.state.user


async def test_context_state_sync_view() -> None:
    app = Quart(__name__)
    captured = {}

    @app.before_request
    def authenticate() -> None:
        auth_state.user = "alice"

    @app.route("/")
    def index() -> str:
        captured["state"] = auth_state._get_current_object()
        captured["valid"] = auth_state.valid
        return auth_state.user

    test_client = app.test_client()
    response = await test_client.get("/")
    assert (await response.get_data()) == b"alice"
    assert captured["valid"] is True
    # The request has ended, the state must be recognized as invalid
    assert captured["state"].valid is False
    with pytest.raises(RuntimeError):
        captured["state"].user


async def test_context_state_shared_with_copied_context() -> None:
    app = Quart(__name__)
    captured = {}

    @app.before_request
    def authenticate() -> None:
        auth_state.user = "alice"

    @app.route("/")
    async def index() -> str:
        @copy_current_request_context
        async def worker() -> None:
            captured["worker_state"] = auth_state._get_current_object()
            captured["worker_user"] = auth_state.user

        await asyncio.ensure_future(worker())
        captured["root_state"] = auth_state._get_current_object()
        return ""

    test_client = app.test_client()
    await test_client.get("/")
    assert captured["worker_user"] == "alice"
    # The copy shares the state, so it is invalidated with the request
    assert captured["worker_state"] is captured["root_state"]
    assert captured["worker_state"].valid is False
    with pytest.raises(RuntimeError):
        captured["worker_state"].user


async def test_context_state_isolated_between_concurrent_requests() -> None:
    app = Quart(__name__)
    entered = threading.Event()
    resume = threading.Event()
    finished = threading.Event()
    captured = {}

    @app.before_request
    def authenticate() -> None:
        auth_state.user = request.headers["X-User"]

    @app.route("/block")
    def block() -> str:
        captured["blocked_state"] = auth_state._get_current_object()
        entered.set()
        resume.wait(5)
        # This request is still active, its state must be valid and its own
        captured["blocked_user"] = auth_state.user
        finished.set()
        return "done"

    @app.route("/who")
    def who() -> str:
        captured["who_state"] = auth_state._get_current_object()
        return auth_state.user

    test_client = app.test_client()
    task = asyncio.ensure_future(test_client.get("/block", headers={"X-User": "alice"}))
    await asyncio.wait_for(asyncio.to_thread(entered.wait), 1)
    # A new request enters whilst the first is blocked in its sync view
    response = await test_client.get("/who", headers={"X-User": "bob"})
    assert (await response.get_data()) == b"bob"
    assert captured["who_state"].valid is False
    assert captured["blocked_state"].valid is True
    resume.set()
    await asyncio.wait_for(task, 2)
    await asyncio.wait_for(asyncio.to_thread(finished.wait), 1)
    # The blocked thread saw its own identity, not the other request's
    assert captured["blocked_user"] == "alice"
    assert captured["blocked_state"] is not captured["who_state"]
    assert captured["blocked_state"].valid is False


async def test_context_state_invalidated_on_disconnect(http_scope: HTTPScope) -> None:
    app = Quart(__name__)
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    captured = {}

    @app.before_request
    def authenticate() -> None:
        auth_state.user = "alice"

    @app.route("/")
    def index() -> str:
        state = auth_state._get_current_object()
        captured["state"] = state
        captured["user"] = state.user
        entered.set()
        # The thread returns late, after the request was cancelled
        release.wait(5)
        captured["valid_after_cancel"] = state.valid
        try:
            state.user
        except RuntimeError:
            captured["denied"] = True
        finished.set()
        return "late"

    scope = {**http_scope, "headers": []}
    connection = ASGIHTTPConnection(app, scope)
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait({"type": "http.request", "body": b"", "more_body": False})

    async def receive() -> ASGIReceiveEvent:
        return await queue.get()

    async def send(message: ASGISendEvent) -> None:
        pass

    task = asyncio.ensure_future(connection(receive, send))
    await asyncio.wait_for(asyncio.to_thread(entered.wait), 1)
    # The client disconnects, cancelling the request whilst the sync
    # view thread is still running
    queue.put_nowait({"type": "http.disconnect"})
    await asyncio.wait_for(task, 2)
    assert captured["state"].valid is False
    release.set()
    await asyncio.wait_for(asyncio.to_thread(finished.wait), 1)
    assert captured["user"] == "alice"
    assert captured["valid_after_cancel"] is False
    assert captured["denied"] is True


async def test_context_state_streaming_after_request_end() -> None:
    app = Quart(__name__)
    paused = threading.Event()
    resume = threading.Event()
    finished = threading.Event()
    captured = {}

    @app.before_request
    def authenticate() -> None:
        auth_state.user = request.headers["X-User"]

    @app.route("/stream")
    def stream() -> tuple:
        state = auth_state._get_current_object()
        captured["stream_state"] = state

        def gen():
            yield b"a"
            # Iterated later, after the request context has popped
            paused.set()
            resume.wait(5)
            captured["stream_valid"] = state.valid
            try:
                state.user
            except RuntimeError:
                captured["stream_denied"] = True
            finished.set()
            yield b"b"

        return gen(), 200

    @app.route("/who")
    def who() -> str:
        captured["who_state"] = auth_state._get_current_object()
        return auth_state.user

    test_client = app.test_client()
    task = asyncio.ensure_future(test_client.get("/stream", headers={"X-User": "alice"}))
    await asyncio.wait_for(asyncio.to_thread(paused.wait), 1)
    # A new request enters whilst the stream is paused
    response = await test_client.get("/who", headers={"X-User": "bob"})
    assert (await response.get_data()) == b"bob"
    resume.set()
    await asyncio.wait_for(task, 2)
    await asyncio.wait_for(asyncio.to_thread(finished.wait), 1)
    assert captured["stream_valid"] is False
    assert captured["stream_denied"] is True
    assert captured["stream_state"] is not captured["who_state"]
    assert captured["who_state"].valid is False


async def test_context_state_stream_with_context() -> None:
    app = Quart(__name__)
    captured = {}

    @app.before_request
    def authenticate() -> None:
        auth_state.user = "alice"

    @app.route("/")
    async def index() -> tuple:
        @stream_with_context
        async def gen():
            # The request context is available whilst streaming
            captured["has_context"] = has_request_context()
            captured["valid"] = auth_state.valid
            try:
                auth_state.user
            except RuntimeError:
                captured["denied"] = True
            yield b"a"

        return gen(), 200

    test_client = app.test_client()
    response = await test_client.get("/")
    assert (await response.get_data()) == b"a"
    assert captured["has_context"] is True
    # The owning context ended before the stream iterated
    assert captured["valid"] is False
    assert captured["denied"] is True
