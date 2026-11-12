"""Auth cookie migration example.

The backend previously issued the ``legacy_session`` cookie scoped to
``domain=example.com; path=/admin``. It now issues ``session`` scoped to
``domain=app.example.com; path=/``. The ``/admin/auth/migrate`` endpoint
performs a time-limited, rule based migration for clients that still hold
the old credential.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

from itsdangerous import BadSignature
from itsdangerous import URLSafeTimedSerializer
from quart import jsonify
from quart import Quart
from quart import request
from quart.wrappers import Response

LEGACY_COOKIE_NAME = "legacy_session"
LEGACY_COOKIE_DOMAIN = "example.com"
LEGACY_COOKIE_PATH = "/admin"

SESSION_COOKIE_NAME = "session"
SESSION_COOKIE_DOMAIN = "app.example.com"
SESSION_COOKIE_PATH = "/"
SESSION_COOKIE_MAX_AGE = 3600

LEGACY_TOKEN_SALT = "cookie-migration-legacy"
SESSION_TOKEN_SALT = "cookie-migration-session"
LEGACY_TOKEN_MAX_AGE = 30 * 24 * 3600
SESSION_TOKEN_MAX_AGE = SESSION_COOKIE_MAX_AGE

USERS = {
    "alice": "alice-password",
    "bob": "bob-password",
}


def _serializer(app: Quart, salt: str) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(app.config["SECRET_KEY"], salt=salt)


def issue_legacy_token(app: Quart, user_id: str) -> str:
    """Mint a credential in the format used by the old backend."""
    return _serializer(app, LEGACY_TOKEN_SALT).dumps({"sub": user_id})


def issue_session_token(app: Quart, user_id: str) -> str:
    """Mint a credential in the format used by the new backend."""
    return _serializer(app, SESSION_TOKEN_SALT).dumps({"sub": user_id})


def _read_signed_identity(
    app: Quart, cookie_name: str, salt: str, max_age: int
) -> str | None:
    raw_token = request.cookies.get(cookie_name)
    if raw_token is None:
        return None
    try:
        payload = _serializer(app, salt).loads(raw_token, max_age=max_age)
    except BadSignature:
        return None
    user_id = payload.get("sub")
    if not isinstance(user_id, str) or user_id not in USERS:
        return None
    return user_id


def read_legacy_identity(app: Quart) -> str | None:
    return _read_signed_identity(
        app, LEGACY_COOKIE_NAME, LEGACY_TOKEN_SALT, LEGACY_TOKEN_MAX_AGE
    )


def read_session_identity(app: Quart) -> str | None:
    return _read_signed_identity(
        app, SESSION_COOKIE_NAME, SESSION_TOKEN_SALT, SESSION_TOKEN_MAX_AGE
    )


def set_session_cookie(response: Response, app: Quart, user_id: str) -> None:
    """Write the new credential with its explicit domain and path."""
    token = issue_session_token(app, user_id)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=SESSION_COOKIE_MAX_AGE,
        domain=SESSION_COOKIE_DOMAIN,
        path=SESSION_COOKIE_PATH,
        httponly=True,
        samesite="Lax",
    )


def expire_legacy_cookie(response: Response) -> None:
    """Expire the old credential at its exact domain and path."""
    response.delete_cookie(
        LEGACY_COOKIE_NAME,
        domain=LEGACY_COOKIE_DOMAIN,
        path=LEGACY_COOKIE_PATH,
    )


def expire_session_cookie(response: Response) -> None:
    """Expire the new credential at its exact domain and path."""
    response.delete_cookie(
        SESSION_COOKIE_NAME,
        domain=SESSION_COOKIE_DOMAIN,
        path=SESSION_COOKIE_PATH,
    )


def migration_window_open(app: Quart) -> bool:
    """Return whether the migration deadline has not passed."""
    deadline: datetime | None = app.config["MIGRATION_DEADLINE"]
    return deadline is None or datetime.now(timezone.utc) < deadline


async def _login_payload() -> dict[str, Any]:
    json_payload = await request.get_json(silent=True)
    if isinstance(json_payload, dict):
        return json_payload
    form = await request.form
    return form.to_dict()


def create_app() -> Quart:
    app = Quart(__name__)
    app.config.update(
        SECRET_KEY="cookie-migration-secret",
        MIGRATION_DEADLINE=datetime(2027, 1, 1, tzinfo=timezone.utc),
    )

    @app.post("/login")
    async def login() -> Response:
        payload = await _login_payload()
        user_id = payload.get("user_id")
        password = payload.get("password")
        if not isinstance(user_id, str) or USERS.get(user_id) != password:
            return jsonify(error="invalid_credentials"), 401

        response = jsonify(user=user_id)
        set_session_cookie(response, app, user_id)
        return response

    @app.post("/logout")
    async def logout() -> Response:
        response = Response(status=204)
        expire_session_cookie(response)
        expire_legacy_cookie(response)
        return response

    @app.post(LEGACY_COOKIE_PATH + "/auth/migrate")
    async def migrate() -> Response:
        if not migration_window_open(app):
            return jsonify(error="migration_window_closed"), 410

        legacy_identity = read_legacy_identity(app)
        session_identity = read_session_identity(app)

        if legacy_identity is None and session_identity is None:
            return jsonify(error="authentication_required"), 401

        if legacy_identity is not None and session_identity is not None:
            if legacy_identity != session_identity:
                # Both credentials are present but disagree. Never pick a
                # winner: force the client to re-authenticate.
                return jsonify(error="identity_conflict"), 409
            target_identity = session_identity
        elif legacy_identity is not None:
            target_identity = legacy_identity
        else:
            # Only the new credential exists; nothing to migrate.
            return jsonify(user=session_identity)

        response = jsonify(user=target_identity)
        set_session_cookie(response, app, target_identity)
        expire_legacy_cookie(response)
        return response

    @app.get("/whoami")
    async def whoami() -> Response:
        session_identity = read_session_identity(app)
        if session_identity is None:
            return jsonify(error="authentication_required"), 401
        return jsonify(user=session_identity)

    return app


app = create_app()
