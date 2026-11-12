from __future__ import annotations

from http.cookies import SimpleCookie

from hypercorn.typing import HTTPScope
from itsdangerous import URLSafeTimedSerializer
from werkzeug.datastructures import Headers

from quart import session
from quart import websocket
from quart.app import Quart
from quart.sessions import SecureCookieSession
from quart.sessions import SecureCookieSessionInterface
from quart.sessions import SessionMixin
from quart.testing import no_op_push
from quart.testing.connections import WebsocketResponseError
from quart.wrappers import Request
from quart.wrappers import Response


async def test_secure_cookie_session_interface_open_session(
    http_scope: HTTPScope,
) -> None:
    session = SecureCookieSession()
    session["something"] = "else"
    interface = SecureCookieSessionInterface()
    app = Quart(__name__)
    app.secret_key = "secret"
    response = Response("")
    await interface.save_session(app, session, response)
    request = Request(
        "GET",
        "http",
        "/",
        b"",
        Headers(),
        "",
        "1.1",
        http_scope,
        send_push_promise=no_op_push,
    )
    request.headers["Cookie"] = response.headers["Set-Cookie"]
    new_session = await interface.open_session(app, request)
    assert new_session == session


async def test_secure_cookie_session_interface_save_session() -> None:
    session = SecureCookieSession()
    session["something"] = "else"
    interface = SecureCookieSessionInterface()
    app = Quart(__name__)
    app.secret_key = "secret"
    response = Response("")
    await interface.save_session(app, session, response)
    cookies: SimpleCookie = SimpleCookie()
    cookies.load(response.headers["Set-Cookie"])
    cookie = cookies[app.config["SESSION_COOKIE_NAME"]]
    assert cookie["path"] == interface.get_cookie_path(app)
    assert cookie["httponly"] == "" if not interface.get_cookie_httponly(app) else True
    assert cookie["secure"] == "" if not interface.get_cookie_secure(app) else True
    assert cookie["samesite"] == (interface.get_cookie_samesite(app) or "")
    assert cookie["domain"] == (interface.get_cookie_domain(app) or "")
    assert cookie["expires"] == (interface.get_expiration_time(app, session) or "")
    assert response.headers["Vary"] == "Cookie"


async def _save_session(session: SecureCookieSession) -> Response:
    interface = SecureCookieSessionInterface()
    app = Quart(__name__)
    app.secret_key = "secret"
    response = Response("")
    await interface.save_session(app, session, response)
    return response


async def test_secure_cookie_session_interface_save_session_no_modification() -> None:
    session = SecureCookieSession()
    session["something"] = "else"
    session.modified = False
    response = await _save_session(session)
    assert response.headers.get("Set-Cookie") is None


async def test_secure_cookie_session_interface_save_session_no_access() -> None:
    session = SecureCookieSession()
    session["something"] = "else"
    session.accessed = False
    session.modified = False
    response = await _save_session(session)
    assert response.headers.get("Set-Cookie") is None
    assert response.headers.get("Vary") is None


class _CountingInterface(SecureCookieSessionInterface):
    def __init__(self) -> None:
        self.resign_count = 0

    def should_resign(self, app: Quart, session: SessionMixin) -> bool:
        if super().should_resign(app, session):
            self.resign_count += 1
            return True
        return False


def _signed_cookie(key: str, data: dict) -> str:
    interface = SecureCookieSessionInterface()
    serializer = URLSafeTimedSerializer(
        key,
        salt=interface.salt,
        serializer=interface.serializer,
        signer_kwargs={
            "key_derivation": interface.key_derivation,
            "digest_method": interface.digest_method,
        },
    )
    return serializer.dumps(data)


def _migration_app() -> tuple[Quart, _CountingInterface]:
    interface = _CountingInterface()
    app = Quart(__name__)
    app.secret_key = "new"
    app.config["SECRET_KEY_FALLBACKS"] = ["old"]
    app.session_interface = interface

    @app.get("/")
    async def index() -> str:
        return interface.get_verification_source(session) or "none"

    @app.get("/error")
    async def error() -> str:
        raise ValueError("boom")

    @app.errorhandler(500)
    async def handle_error(error: Exception) -> tuple[str, int]:
        return interface.get_verification_source(session) or "none", 500

    @app.websocket("/ws/accept")
    async def ws_accept() -> None:
        await websocket.send(interface.get_verification_source(session) or "none")

    @app.websocket("/ws/reject")
    async def ws_reject() -> tuple[str, int]:
        return interface.get_verification_source(session) or "none", 401

    return app, interface


async def test_resign_on_normal_response() -> None:
    app, interface = _migration_app()
    client = app.test_client()
    client.set_cookie("localhost", "session", _signed_cookie("old", {"user": "1"}))

    response = await client.get("/")
    assert await response.get_data(as_text=True) == "fallback"
    assert "Set-Cookie" in response.headers
    assert interface.resign_count == 1

    # The client now sends the re-signed cookie, verified by the current key
    response = await client.get("/")
    assert await response.get_data(as_text=True) == "current"
    assert "Set-Cookie" not in response.headers
    assert interface.resign_count == 1


async def test_resign_in_error_handler() -> None:
    app, interface = _migration_app()
    client = app.test_client()
    client.set_cookie("localhost", "session", _signed_cookie("old", {"user": "1"}))

    response = await client.get("/error")
    assert response.status_code == 500
    assert await response.get_data(as_text=True) == "fallback"
    assert "Set-Cookie" in response.headers
    assert interface.resign_count == 1

    response = await client.get("/")
    assert await response.get_data(as_text=True) == "current"
    assert interface.resign_count == 1


async def test_resign_on_unaccepted_websocket() -> None:
    app, interface = _migration_app()
    client = app.test_client()
    client.set_cookie("localhost", "session", _signed_cookie("old", {"user": "1"}))

    response = None
    try:
        async with client.websocket("/ws/reject"):
            pass
    except WebsocketResponseError as error:
        response = error.response
    assert response is not None
    assert response.status_code == 401
    assert "Set-Cookie" in response.headers
    assert interface.resign_count == 1

    cookies: SimpleCookie = SimpleCookie()
    cookies.load(response.headers["Set-Cookie"])
    client.set_cookie("localhost", "session", cookies["session"].value)
    follow_up = await client.get("/")
    assert await follow_up.get_data(as_text=True) == "current"
    assert interface.resign_count == 1


async def test_no_resign_on_accepted_websocket() -> None:
    app, interface = _migration_app()
    client = app.test_client()
    client.set_cookie("localhost", "session", _signed_cookie("old", {"user": "1"}))

    async with client.websocket("/ws/accept") as test_websocket:
        source = await test_websocket.receive()
    assert source == "fallback"
    # An accepted websocket cannot deliver a cookie, so nothing is resigned
    assert interface.resign_count == 0

    # The client still uses the old key and is migrated on a HTTP request
    response = await client.get("/")
    assert await response.get_data(as_text=True) == "fallback"
    assert "Set-Cookie" in response.headers
    assert interface.resign_count == 1

    response = await client.get("/")
    assert await response.get_data(as_text=True) == "current"
    assert interface.resign_count == 1


async def test_retired_key_is_not_resigned() -> None:
    interface = _CountingInterface()
    app = Quart(__name__)
    app.secret_key = "new"  # The old key is retired, no fallbacks remain
    app.session_interface = interface

    @app.get("/")
    async def index() -> str:
        user = session.get("user")
        return f"{interface.get_verification_source(session) or 'none'}:{user}"

    client = app.test_client()
    client.set_cookie("localhost", "session", _signed_cookie("old", {"user": "1"}))

    response = await client.get("/")
    # The unverifiable identity must not be kept nor renewed
    assert await response.get_data(as_text=True) == "none:None"
    assert "Set-Cookie" not in response.headers
    assert interface.resign_count == 0

    response = await client.get("/")
    assert await response.get_data(as_text=True) == "none:None"
    assert interface.resign_count == 0
