"""Executable migration checks against real Quart context entry points.

Run:  .venv/bin/python examples/auth_context_migration/run_checks.py

Scenarios compare normal exit, exception exit, nested/copied contexts and
parallel tasks, for both request and websocket contexts, and prove that a
revoked identity is never re-activated by an inner cache.
"""

from __future__ import annotations

import asyncio

from quart import copy_current_request_context
from quart import copy_current_websocket_context
from quart import has_request_context
from quart import has_websocket_context
from quart import Quart
from werkzeug.datastructures import Headers

from auth_extension import AuthExtension
from auth_extension import Identity


def make_app() -> tuple[Quart, AuthExtension]:
    app = Quart(__name__)
    auth = AuthExtension(app)
    return app, auth


def make_websocket(path: str = "/ws?owner=carol", token: str = "tok-carol"):
    from quart.wrappers import Websocket

    headers = Headers({"Authorization": f"Bearer {token}"})
    scope = {
        "type": "websocket",
        "path": path.split("?")[0],
        "query_string": path.split("?")[1].encode(),
        "headers": [],
        "scheme": "ws",
        "subprotocols": [],
    }

    async def _send(data):  # noqa: ARG001
        pass

    async def _accept():
        pass

    async def _close():
        pass

    return Websocket(
        scope["path"], scope["query_string"], "ws", headers, "", "1.1", [],
        _send, _accept, _close, scope,
    )


async def scenario_request_normal_exit() -> None:
    app, auth = make_app()
    async with app.test_request_context(
        "/resource?owner=alice", headers={"Authorization": "Bearer tok-alice"}
    ):
        first = await auth.current_identity()
        second = await auth.current_identity()
        assert first is second, "L1 memo must dedupe within one context"
        assert isinstance(first, Identity)
        assert first.account_id == "account-of-tok-alice"
        assert first.resource_owner_id == "owner-alice"
        assert first.account_id != first.resource_owner_id
    assert auth.load_count == 1, "loader must run exactly once"
    assert not has_request_context()
    assert auth.teardowns[-1] == ("request", None), "teardown with exc=None"
    print("PASS request normal exit: cached once, teardown exc=None")


async def scenario_request_exception_exit() -> None:
    app, auth = make_app()
    with_error = RuntimeError("boom")
    try:
        async with app.test_request_context(
            "/resource?owner=bob", headers={"Authorization": "Bearer tok-bob"}
        ):
            identity = await auth.current_identity()
            assert identity is not None
            raise with_error
    except RuntimeError:
        pass
    kind, exc = auth.teardowns[-1]
    assert kind == "request" and exc is with_error, "teardown must receive the exception"
    # A fresh context must not see any leaked cache from the failed one.
    async with app.test_request_context(
        "/resource?owner=bob", headers={"Authorization": "Bearer tok-bob"}
    ):
        again = await auth.current_identity()
        assert again is not None and again.account_id == "account-of-tok-bob"
    assert auth.load_count == 2, "no cache may leak across contexts"
    print("PASS request exception exit: teardown got exc, no cache leak")


async def scenario_websocket_normal_and_exception() -> None:
    app, auth = make_app()
    async with app.websocket_context(make_websocket()):
        identity = await auth.current_identity()
        assert identity is not None
        assert identity.account_id == "account-of-tok-carol"
        assert identity.resource_owner_id == "owner-carol"
        assert (await auth.current_identity()) is identity
    assert auth.load_count == 1
    assert auth.teardowns[-1] == ("websocket", None)

    failure = ValueError("ws-fail")
    try:
        async with app.websocket_context(make_websocket(token="tok-dave")):
            assert (await auth.current_identity()).account_id == "account-of-tok-dave"
            raise failure
    except ValueError:
        pass
    kind, exc = auth.teardowns[-1]
    assert kind == "websocket" and exc is failure
    assert not has_websocket_context()
    print("PASS websocket normal+exception exit: unified teardown path")


async def scenario_nested_copy_no_reactivation() -> None:
    app, auth = make_app()
    async with app.test_request_context(
        "/resource?owner=eve", headers={"Authorization": "Bearer tok-eve"}
    ):
        outer = await auth.current_identity()

        @copy_current_request_context
        async def inner() -> None:
            # Copied context: fresh g, shared request object.
            assert (await auth.current_identity()) is outer, "L2 shared with copies"
            assert auth.load_count == 1, "copy must not re-hit the backend"
            await auth.revoke()  # 内层使凭据失效
            assert await auth.current_identity() is None

        await inner()
        # 失效资源不得被外层/内层缓存重新激活
        assert await auth.current_identity() is None, "revoked identity resurrected!"
        assert auth.load_count == 1, "revoked resource was re-loaded"
    print("PASS nested copied context: revocation propagates, no reactivation")


async def scenario_parallel_tasks() -> None:
    app, auth = make_app()
    async with app.test_request_context(
        "/resource?owner=frank", headers={"Authorization": "Bearer tok-frank"}
    ):
        loaded = asyncio.Event()
        revoked = asyncio.Event()
        observations: list[object] = []

        @copy_current_request_context
        async def reader_a() -> None:
            first = await auth.current_identity()  # performs the single load
            assert first is not None
            loaded.set()
            await revoked.wait()
            observations.append(await auth.current_identity())

        @copy_current_request_context
        async def revoker() -> None:
            await loaded.wait()
            await auth.revoke()
            revoked.set()

        @copy_current_request_context
        async def reader_c() -> None:
            await revoked.wait()
            observations.append(await auth.current_identity())

        await asyncio.gather(reader_a(), revoker(), reader_c())
        assert observations == [None, None], f"inner caches reactivated: {observations}"
        assert auth.load_count == 1, "parallel tasks must share one load"
        assert await auth.current_identity() is None
    print("PASS parallel tasks: single load, revocation visible to all")


async def scenario_websocket_copy_revoke() -> None:
    app, auth = make_app()
    async with app.websocket_context(make_websocket(token="tok-gina")):
        outer = await auth.current_identity()

        @copy_current_websocket_context
        async def inner() -> None:
            assert (await auth.current_identity()) is outer
            await auth.revoke()

        await inner()
        assert await auth.current_identity() is None
        assert auth.load_count == 1
    assert auth.teardowns[-1] == ("websocket", None)
    print("PASS websocket copied context: revocation propagates")


async def scenario_exception_after_revoke() -> None:
    app, auth = make_app()
    failure = RuntimeError("post-revoke failure")
    try:
        async with app.test_request_context(
            "/resource?owner=heidi", headers={"Authorization": "Bearer tok-heidi"}
        ):
            assert await auth.current_identity() is not None
            await auth.revoke()
            raise failure
    except RuntimeError:
        pass
    kind, exc = auth.teardowns[-1]
    assert kind == "request" and exc is failure
    # Backend still refuses the token: revocation survives the failed context.
    assert "tok-heidi" in auth._revoked_tokens
    print("PASS exception after revoke: teardown got exc, revocation survives")


async def main() -> None:
    await scenario_request_normal_exit()
    await scenario_request_exception_exit()
    await scenario_websocket_normal_and_exception()
    await scenario_nested_copy_no_reactivation()
    await scenario_parallel_tasks()
    await scenario_websocket_copy_revoke()
    await scenario_exception_after_revoke()
    print("\nAll migration checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
