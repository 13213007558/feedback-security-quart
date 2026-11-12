Cookie migration
================

This application shows how to move a backend auth cookie from a legacy
name, domain and path to a new configuration.

The old backend issued ``legacy_session`` scoped to
``domain=example.com; path=/admin``. The new backend issues ``session``
scoped to ``domain=app.example.com; path=/``.

``POST /admin/auth/migrate`` is the time-limited migration entry point.
Only enabled while ``MIGRATION_DEADLINE`` is in the future, it:

1. Reads and verifies the legacy credential (a signed itsdangerous
   token) and, when present, the new credential.
2. Returns ``409 identity_conflict`` when both are present but identify
   different users; it never picks a winner and writes no cookies.
3. Writes a fresh ``session`` token at the new domain/path and expires
   ``legacy_session`` at its exact original domain/path.
4. Leaves the cookie jar untouched on any failure response
   (``401``/``409``/``410``).

``POST /login`` writes only the new cookie and ``POST /logout`` expires
both names at their respective scopes.

The tests drive the app through Quart's test client and assert against
the client's real ``CookieJar`` (including the ``Cookie`` header that
would be sent on subsequent requests), rather than counting
``Set-Cookie`` response headers.

Run with::

    cd examples/cookie_migration
    python -m pytest -o addopts=
