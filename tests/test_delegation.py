from __future__ import annotations

import asyncio

import pytest

from quart import CapabilityDeniedError
from quart import copy_current_request_context
from quart import current_capabilities
from quart import current_delegation
from quart import delegate
from quart import has_capability
from quart import is_delegated
from quart import Quart
from quart import require_capability
from quart import session
from quart.signals import got_background_exception


async def test_request_identity_reads_unchanged_when_delegated() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"

    @app.route("/")
    async def index() -> str:
        session["account"] = "alice"
        assert not is_delegated()
        with delegate({"read"}, subject="alice"):
            assert is_delegated()
            # Identity reads are unaffected by the delegation boundary.
            assert session["account"] == "alice"
        assert not is_delegated()
        return session["account"]

    async with app.test_app():
        response = await app.test_client().get("/")
    assert (await response.get_data(as_text=True)) == "alice"


async def test_nested_delegate_scopes_restore_on_exit() -> None:
    app = Quart(__name__)

    @app.route("/")
    async def index() -> str:
        assert current_capabilities() is None
        with delegate({"read", "write"}, label="outer"):
            assert current_capabilities() == {"read", "write"}
            with delegate({"read"}, label="inner"):
                assert current_capabilities() == {"read"}
                assert current_delegation().depth == 1
            assert current_capabilities() == {"read", "write"}
            assert current_delegation().depth == 0
        assert current_capabilities() is None
        assert current_delegation() is None
        return ""

    async with app.test_app():
        response = await app.test_client().get("/")
    assert response.status_code == 200


async def test_delegate_scope_restored_after_failure() -> None:
    app = Quart(__name__)

    @app.route("/")
    async def index() -> str:
        with delegate({"read", "write"}):
            with pytest.raises(RuntimeError):
                with delegate({"read"}):
                    raise RuntimeError("boom")
            # The failed inner scope was cleaned up, the outer remains.
            assert current_capabilities() == {"read", "write"}
        assert current_delegation() is None
        return ""

    async with app.test_app():
        response = await app.test_client().get("/")
    assert response.status_code == 200


async def test_delegate_cannot_widen_and_denial_does_not_leak() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"

    @app.route("/sensitive")
    async def index() -> str:
        session["account"] = "alice"
        with delegate({"read"}, subject="alice"):
            with pytest.raises(CapabilityDeniedError) as exc_info:
                with delegate({"read", "admin"}):
                    pass
        error = exc_info.value
        assert error.missing == {"admin"}
        # The missing capability is named...
        assert "admin" in str(error)
        # ...but parent request data is not leaked.
        assert "alice" not in str(error)
        assert "/sensitive" not in str(error)
        return ""

    async with app.test_app():
        response = await app.test_client().get("/sensitive")
    assert response.status_code == 200


async def test_require_capability_distinguishes_original_and_delegated() -> None:
    app = Quart(__name__)

    def protected_operation() -> str:
        require_capability("write")
        return "delegated" if is_delegated() else "original"

    @app.route("/")
    async def index() -> str:
        # Original authorization: unrestricted, and distinguishable.
        assert protected_operation() == "original"
        assert has_capability("write")
        with delegate({"read"}):
            assert not has_capability("write")
            with pytest.raises(CapabilityDeniedError) as exc_info:
                protected_operation()
            assert exc_info.value.missing == {"write"}
        with delegate({"read", "write"}):
            assert protected_operation() == "delegated"
        return ""

    async with app.test_app():
        response = await app.test_client().get("/")
    assert response.status_code == 200


async def test_copied_context_cannot_restore_full_identity() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"

    @app.route("/")
    async def index() -> str:
        session["account"] = "alice"

        @copy_current_request_context
        async def copied() -> str:
            # Re-copying the context does not escape the delegation.
            assert is_delegated()
            assert current_capabilities() == {"read"}
            assert session["account"] == "alice"
            with pytest.raises(CapabilityDeniedError):
                require_capability("write")
            return "copied"

        with delegate({"read"}):
            assert await copied() == "copied"
        return ""

    async with app.test_app():
        response = await app.test_client().get("/")
    assert response.status_code == 200


async def test_three_level_background_task_delegation_two_accounts() -> None:
    app = Quart(__name__)
    app.secret_key = "secret"
    records: list[dict] = []
    denials: list[CapabilityDeniedError] = []
    done: dict[tuple[str, int], asyncio.Event] = {}

    async def record_denial(
        sender: Quart, *, exception: Exception, **extra: object
    ) -> None:
        if isinstance(exception, CapabilityDeniedError):
            denials.append(exception)

    got_background_exception.connect(record_denial, app, weak=False)

    def snapshot(account: str, level: int) -> dict:
        delegation = current_delegation()
        return {
            "account": account,
            "level": level,
            "delegated": is_delegated(),
            "capabilities": current_capabilities(),
            "depth": delegation.depth,
            "provenance": [
                (node.label, set(node.capabilities)) for node in delegation.provenance()
            ],
        }

    async def level3(account: str) -> None:
        records.append(snapshot(account, 3))
        # A level-3 task cannot widen back to capabilities its parent
        # was never granted; the denial is reported as a background
        # exception naming only the missing capability.
        app.add_background_task(level4, account, capabilities={"read", "admin"})
        done[(account, 3)].set()

    async def level4(account: str) -> None:
        records.append(snapshot(account, 4))  # pragma: no cover

    async def level2(account: str) -> None:
        records.append(snapshot(account, 2))
        app.add_background_task(level3, account, capabilities={"read"})
        await done[(account, 3)].wait()
        done[(account, 2)].set()

    async def level1(account: str) -> None:
        records.append(snapshot(account, 1))
        app.add_background_task(level2, account, capabilities={"read", "write"})
        await done[(account, 2)].wait()

    grants = {
        "alice": {"read", "write", "audit"},
        "bob": {"read", "write", "billing"},
    }

    @app.route("/start/<account>")
    async def start(account: str) -> str:
        session["account"] = account
        assert session["account"] == account
        app.add_background_task(level1, account, capabilities=grants[account])
        return ""

    for account in grants:
        for level in (2, 3):
            done[(account, level)] = asyncio.Event()

    async with app.test_app():
        client = app.test_client()
        await client.get("/start/alice")
        await client.get("/start/bob")

    for account, root_capability in (("alice", "audit"), ("bob", "billing")):
        chain = [
            next(r for r in records if r["account"] == account and r["level"] == level)
            for level in (1, 2, 3)
        ]
        # Every level is delegated, never the original authorization.
        assert all(entry["delegated"] for entry in chain)
        # Capabilities only narrow down the three levels.
        assert chain[0]["capabilities"] == {"read", "write", root_capability}
        assert chain[1]["capabilities"] == {"read", "write"}
        assert chain[2]["capabilities"] == {"read"}
        assert [entry["depth"] for entry in chain] == [0, 1, 2]
        # Provenance records each grant from the root down.
        provenance = chain[2]["provenance"]
        assert [label for label, _ in provenance] == [
            "background:level1",
            "background:level2",
            "background:level3",
        ]
        assert [caps for _, caps in provenance] == [
            {"read", "write", root_capability},
            {"read", "write"},
            {"read"},
        ]

    # The level-4 widening attempt was denied for both accounts, naming
    # only the missing capability.
    assert len(denials) == 2
    for denial in denials:
        assert denial.missing == {"admin"}
        assert "admin" in str(denial)
        assert "alice" not in str(denial)
        assert "bob" not in str(denial)
    assert not any(record["level"] == 4 for record in records)


async def test_background_task_failure_cleanup() -> None:
    app = Quart(__name__)
    exceptions: list[Exception] = []

    async def record(sender: Quart, *, exception: Exception, **extra: object) -> None:
        exceptions.append(exception)

    got_background_exception.connect(record, app, weak=False)

    async def failing() -> None:
        assert current_capabilities() == {"read"}
        raise RuntimeError("task failed")

    @app.route("/")
    async def index() -> str:
        app.add_background_task(failing, capabilities={"read"})
        # The requesting task is unaffected by the delegated child.
        assert not is_delegated()
        return ""

    async with app.test_app():
        await app.test_client().get("/")

    assert len(exceptions) == 1
    assert isinstance(exceptions[0], RuntimeError)
    assert not is_delegated()
