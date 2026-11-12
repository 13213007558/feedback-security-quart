from datetime import datetime
from datetime import timezone

import pytest

from cookie_migration import create_app
from cookie_migration import LEGACY_COOKIE_DOMAIN
from cookie_migration import LEGACY_COOKIE_PATH
from cookie_migration import issue_legacy_token
from cookie_migration import issue_session_token
from cookie_migration import SESSION_COOKIE_DOMAIN
from cookie_migration import SESSION_COOKIE_PATH


@pytest.fixture()
def app():
    app = create_app()
    app.config["MIGRATION_DEADLINE"] = datetime(
        2027, 1, 1, tzinfo=timezone.utc
    )
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def seed_legacy_cookie(client, app, user_id):
    client.set_cookie(
        "example.com",
        "legacy_session",
        issue_legacy_token(app, user_id),
        domain=LEGACY_COOKIE_DOMAIN,
        path=LEGACY_COOKIE_PATH,
    )


def seed_session_cookie(client, app, user_id):
    client.set_cookie(
        SESSION_COOKIE_DOMAIN,
        "session",
        issue_session_token(app, user_id),
        domain=SESSION_COOKIE_DOMAIN,
        path=SESSION_COOKIE_PATH,
    )
