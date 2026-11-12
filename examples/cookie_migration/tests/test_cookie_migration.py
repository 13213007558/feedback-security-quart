from datetime import datetime
from datetime import timezone
from urllib.request import Request as U2Request

from conftest import seed_legacy_cookie
from conftest import seed_session_cookie

from cookie_migration import LEGACY_COOKIE_DOMAIN
from cookie_migration import LEGACY_COOKIE_NAME
from cookie_migration import LEGACY_COOKIE_PATH
from cookie_migration import SESSION_COOKIE_DOMAIN
from cookie_migration import SESSION_COOKIE_NAME
from cookie_migration import SESSION_COOKIE_PATH

HOST = "app.example.com"
MIGRATE_PATH = LEGACY_COOKIE_PATH + "/auth/migrate"


def jar_cookies(client):
    """Return the cookies the client actually keeps, with full scoping.

    CookieJar stores domain cookies with a leading dot; normalize it so
    the assertions describe the logical domain that was configured.
    """
    return {
        (cookie.name, cookie.domain.lstrip("."), cookie.path): cookie
        for cookie in client.cookie_jar
    }


def sent_cookie_header(client, url):
    """Return the Cookie header the jar would attach to a real request."""
    request = U2Request(url)
    client.cookie_jar.add_cookie_header(request)
    return request.get_header("Cookie")


def set_cookie_headers(response):
    return response.headers.get_all("set-cookie")


async def test_migration_rewrites_credential_and_clears_old_scope(client, app):
    seed_legacy_cookie(client, app, "alice")

    response = await client.post(
        MIGRATE_PATH, headers={"host": HOST}
    )

    assert response.status_code == 200
    cookies = jar_cookies(client)

    # The legacy cookie must be gone from the jar, not just overwritten.
    assert (
        LEGACY_COOKIE_NAME,
        LEGACY_COOKIE_DOMAIN,
        LEGACY_COOKIE_PATH,
    ) not in cookies

    # The new cookie is retained at the new domain and path.
    new_cookie = cookies[
        (SESSION_COOKIE_NAME, SESSION_COOKIE_DOMAIN, SESSION_COOKIE_PATH)
    ]
    assert new_cookie.value

    # Subsequent requests present only the new credential.
    header = sent_cookie_header(
        client, f"http://{HOST}/whoami"
    )
    assert header.startswith(f"{SESSION_COOKIE_NAME}=")
    assert LEGACY_COOKIE_NAME not in header

    whoami = await client.get("/whoami", headers={"host": HOST})
    assert whoami.status_code == 200
    assert (await whoami.get_json())["user"] == "alice"

    # The response must expire the old cookie at its exact domain/path.
    expire_header = next(
        header
        for header in set_cookie_headers(response)
        if header.startswith(LEGACY_COOKIE_NAME + "=")
    )
    assert f"domain={LEGACY_COOKIE_DOMAIN}" in expire_header.lower()
    assert f"path={LEGACY_COOKIE_PATH}" in expire_header.lower()
    assert "max-age=0" in expire_header.lower().replace(" ", "")


async def test_login_logout_round_trip_through_cookie_jar(client):
    response = await client.post(
        "/login",
        json={"user_id": "bob", "password": "bob-password"},
        headers={"host": HOST},
    )
    assert response.status_code == 200

    cookies = jar_cookies(client)
    assert set(cookies) == {
        (SESSION_COOKIE_NAME, SESSION_COOKIE_DOMAIN, SESSION_COOKIE_PATH)
    }
    new_cookie = next(iter(cookies.values()))
    assert new_cookie.has_nonstandard_attr("HttpOnly")
    assert new_cookie.get_nonstandard_attr("SameSite") == "Lax"

    whoami = await client.get("/whoami", headers={"host": HOST})
    assert (await whoami.get_json())["user"] == "bob"

    response = await client.post(
        "/logout", headers={"host": HOST}
    )
    assert response.status_code == 204

    # Logout must clear both names at their respective scopes, and the
    # client jar must actually be empty afterwards.
    assert jar_cookies(client) == {}
    assert (
        sent_cookie_header(client, f"http://{HOST}/whoami") is None
    )

    whoami = await client.get("/whoami", headers={"host": HOST})
    assert whoami.status_code == 401

    logout_headers = set_cookie_headers(response)
    session_expiry = next(
        header
        for header in logout_headers
        if header.startswith(SESSION_COOKIE_NAME + "=")
    )
    legacy_expiry = next(
        header
        for header in logout_headers
        if header.startswith(LEGACY_COOKIE_NAME + "=")
    )
    assert f"domain={SESSION_COOKIE_DOMAIN}" in session_expiry.lower()
    assert f"path={SESSION_COOKIE_PATH}" in session_expiry.lower()
    assert f"domain={LEGACY_COOKIE_DOMAIN}" in legacy_expiry.lower()
    assert f"path={LEGACY_COOKIE_PATH}" in legacy_expiry.lower()


async def test_conflicting_identities_are_rejected_not_guessed(
    client, app
):
    seed_legacy_cookie(client, app, "alice")
    seed_session_cookie(client, app, "bob")

    response = await client.post(
        MIGRATE_PATH, headers={"host": HOST}
    )

    assert response.status_code == 409
    assert (await response.get_json())["error"] == "identity_conflict"

    # Neither credential is written, rotated, nor deleted on conflict.
    cookies = jar_cookies(client)
    assert set(cookies) == {
        (LEGACY_COOKIE_NAME, LEGACY_COOKIE_DOMAIN, LEGACY_COOKIE_PATH),
        (SESSION_COOKIE_NAME, SESSION_COOKIE_DOMAIN, SESSION_COOKIE_PATH),
    }
    assert set_cookie_headers(response) == []

    # Both original cookies continue to be sent by the client.
    header = sent_cookie_header(client, f"http://{HOST}{MIGRATE_PATH}")
    assert f"{LEGACY_COOKIE_NAME}=" in header
    assert f"{SESSION_COOKIE_NAME}=" in header


async def test_same_identity_on_both_cookies_rotates_and_clears_legacy(
    client, app
):
    seed_legacy_cookie(client, app, "alice")
    seed_session_cookie(client, app, "alice")

    response = await client.post(
        MIGRATE_PATH, headers={"host": HOST}
    )

    assert response.status_code == 200
    cookies = jar_cookies(client)
    assert set(cookies) == {
        (SESSION_COOKIE_NAME, SESSION_COOKIE_DOMAIN, SESSION_COOKIE_PATH)
    }
    assert (
        sent_cookie_header(client, f"http://{HOST}/whoami")
        == f"{SESSION_COOKIE_NAME}={next(iter(cookies.values())).value}"
    )


async def test_forged_legacy_cookie_fails_without_touching_jar(client):
    client.set_cookie(
        "example.com",
        LEGACY_COOKIE_NAME,
        "not-a-valid-token",
        domain=LEGACY_COOKIE_DOMAIN,
        path=LEGACY_COOKIE_PATH,
    )

    response = await client.post(
        MIGRATE_PATH, headers={"host": HOST}
    )

    assert response.status_code == 401
    assert set_cookie_headers(response) == []
    assert set(jar_cookies(client)) == {
        (LEGACY_COOKIE_NAME, LEGACY_COOKIE_DOMAIN, LEGACY_COOKIE_PATH)
    }


async def test_missing_credentials_failure_response_changes_nothing(
    client,
):
    response = await client.post(
        MIGRATE_PATH, headers={"host": HOST}
    )

    assert response.status_code == 401
    assert (await response.get_json())["error"] == "authentication_required"
    assert set_cookie_headers(response) == []
    assert jar_cookies(client) == {}


async def test_migration_window_closed_returns_410_and_keeps_cookies(
    client, app
):
    seed_legacy_cookie(client, app, "alice")
    app.config["MIGRATION_DEADLINE"] = datetime(
        2020, 1, 1, tzinfo=timezone.utc
    )

    response = await client.post(
        MIGRATE_PATH, headers={"host": HOST}
    )

    assert response.status_code == 410
    assert (await response.get_json())["error"] == "migration_window_closed"
    assert set_cookie_headers(response) == []
    assert set(jar_cookies(client)) == {
        (LEGACY_COOKIE_NAME, LEGACY_COOKIE_DOMAIN, LEGACY_COOKIE_PATH)
    }


async def test_only_new_cookie_is_a_noop(client, app):
    seed_session_cookie(client, app, "bob")

    response = await client.post(
        MIGRATE_PATH, headers={"host": HOST}
    )

    assert response.status_code == 200
    assert set_cookie_headers(response) == []
    assert set(jar_cookies(client)) == {
        (SESSION_COOKIE_NAME, SESSION_COOKIE_DOMAIN, SESSION_COOKIE_PATH)
    }
    assert (await response.get_json())["user"] == "bob"


async def test_failed_login_sets_no_cookie(client):
    response = await client.post(
        "/login",
        json={"user_id": "alice", "password": "wrong"},
        headers={"host": HOST},
    )

    assert response.status_code == 401
    assert set_cookie_headers(response) == []
    assert jar_cookies(client) == {}
