from __future__ import annotations

import asyncio
import gc
from pathlib import Path

import pytest

from quart import Blueprint
from quart import Quart
from quart import ReadOnlyAccessError
from quart import RenderScopeClosedError
from quart import RenderScopeDeniedError
from quart import Response
from quart import ResponseReturnValue
from quart import render_scoped
from quart import render_template
from quart import render_template_string
from quart import stream_template
from quart import stream_template_string


class AccountAccess:
    def __init__(self, account: str, log: list[str]) -> None:
        self.account = account
        self.log = log
        self.calls: list[str] = []

    def read(self, key: str) -> str:
        self.calls.append(key)
        self.log.append(f"read:{self.account}:{key}")
        return f"{self.account}:{key}"

    async def aread(self, key: str) -> str:
        await asyncio.sleep(0)
        return self.read(key)

    async def aclose(self) -> None:
        self.log.append(f"close:{self.account}")


def make_app(log: list[str] | None = None) -> Quart:
    app = Quart(__name__)
    log = [] if log is None else log

    @app.context_processor
    async def provide_access() -> dict:
        return {"access": render_scoped(AccountAccess("unknown", log))}

    return app


async def test_render_scope_available_only_during_render() -> None:
    app = make_app()
    holder = {}

    @app.context_processor
    async def capture() -> dict:
        def record(value: object) -> str:
            holder.setdefault("access", value)
            return ""

        return {"capture": record}

    async with app.app_context():
        assert await render_template_string("{{ access.read('a') }}") == "unknown:a"
        # A fresh render gets a fresh, active access object.
        assert (
            await render_template_string("{{ capture(access) }}{{ access.read('b') }}")
            == "unknown:b"
        )
    with pytest.raises(RenderScopeClosedError):
        holder["access"].read("after")  # type: ignore[attr-defined]


async def test_access_fails_after_scope_even_if_captured() -> None:
    app = make_app()
    holder = {}

    @app.context_processor
    async def capture_access() -> dict:
        def record(value: object) -> str:
            holder["access"] = value
            return ""

        return {"capture": record}

    async with app.app_context():
        await render_template_string("{{ capture(access) }}")
    with pytest.raises(RenderScopeClosedError):
        holder["access"].read("later")  # type: ignore[attr-defined]


async def test_read_only_access_rejects_writes() -> None:
    app = make_app()
    async with app.app_context():
        with pytest.raises(ReadOnlyAccessError):
            await render_template_string("{% set _ = access.__setattr__('x', 1) %}")

    holder = {}

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", []))}

    @app.context_processor
    async def capture() -> dict:
        def record(value: object) -> str:
            holder["access"] = value
            return ""

        return {"capture": record}

    async with app.app_context():
        await render_template_string("{{ capture(access) }}")
    proxy = holder["access"]
    with pytest.raises(ReadOnlyAccessError):
        proxy.account = "b"  # type: ignore[attr-defined]
    with pytest.raises(RenderScopeClosedError):
        proxy.read("a")  # type: ignore[attr-defined]


async def test_scope_released_in_reverse_binding_order() -> None:
    app = Quart(__name__)
    log: list[str] = []

    def release(name: str):
        def inner(value: object) -> None:
            log.append(f"release:{name}")

        return inner

    @app.context_processor
    async def provide() -> dict:
        return {
            "first": render_scoped(
                AccountAccess("first", log), release=release("first")
            ),
            "second": render_scoped(
                AccountAccess("second", log), release=release("second")
            ),
        }

    async with app.app_context():
        await render_template_string("{{ second.read('x') }}{{ first.read('y') }}")
    assert log == ["read:second:x", "read:first:y", "release:second", "release:first"]


async def test_template_exception_releases_and_closes_even_on_error() -> None:
    app = make_app()
    log: list[str] = []

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", log))}

    async with app.app_context():
        with pytest.raises(ZeroDivisionError):
            await render_template_string("{{ access.read('x') }}{{ 1 / 0 }}")
    assert log == ["read:a:x", "close:a"]


async def test_stream_scope_lives_only_for_the_stream() -> None:
    app = make_app()
    log: list[str] = []

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", log))}

    holder = {}

    @app.context_processor
    async def capture() -> dict:
        def record(value: object) -> str:
            holder["access"] = value
            return ""

        return {"capture": record}

    async with app.app_context():
        stream = await stream_template_string(
            "{{ capture(access) }}{{ access.read('one') }}|{{ access.read('two') }}"
        )
        chunks = [chunk async for chunk in stream]
        assert "".join(chunks) == "a:one|a:two"
        with pytest.raises(RenderScopeClosedError):
            holder["access"].read("later")  # type: ignore[attr-defined]
    assert log == ["read:a:one", "read:a:two", "close:a"]


async def test_stream_close_without_exhaustion_releases_scope() -> None:
    app = make_app()
    log: list[str] = []
    holder = {}

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", log))}

    @app.context_processor
    async def capture() -> dict:
        def record(value: object) -> str:
            holder["access"] = value
            return ""

        return {"capture": record}

    async with app.app_context():
        stream = await stream_template_string(
            "{{ capture(access) }}{{ access.read('first') }}|{{ access.read('never') }}"
        )
        assert await stream.__anext__() == ""  # capture() returns an empty string
        assert await stream.__anext__() == "a:first"
        await stream.aclose()
        # Closing twice remains harmless.
        await stream.aclose()
    assert log == ["read:a:first", "close:a"]
    with pytest.raises(RenderScopeClosedError):
        holder["access"].read("later")  # type: ignore[attr-defined]


async def test_abandoned_stream_is_released_without_explicit_close() -> None:
    app = make_app()
    log: list[str] = []
    released = asyncio.Event()

    class Tracked(AccountAccess):
        async def aclose(self) -> None:
            await super().aclose()
            released.set()

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(Tracked("a", log))}

    async with app.app_context():
        stream = await stream_template_string("{{ access.read('one') }}|{{ 'x' }}")
        assert await stream.__anext__() == "a:one"
        del stream
        for _ in range(5):
            gc.collect()
            await asyncio.sleep(0)
        await asyncio.wait_for(released.wait(), 1)
    assert log == ["read:a:one", "close:a"]


async def test_paused_stream_does_not_leave_access_usable() -> None:
    app = make_app()
    holder = {}

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", []))}

    @app.context_processor
    async def capture() -> dict:
        def record(value: object) -> str:
            holder["access"] = value
            return ""

        return {"capture": record}

    async with app.app_context():
        stream = await stream_template_string(
            "{{ capture(access) }}{{ access.read('one') }}|{{ access.read('two') }}"
        )
        assert await stream.__anext__() == ""
        assert await stream.__anext__() == "a:one"
        # Between chunks no render is executing, so the captured proxy
        # must not be usable even though the stream is still open.
        with pytest.raises(RenderScopeClosedError):
            holder["access"].read("between")  # type: ignore[attr-defined]
        assert await stream.__anext__() == "|"
        assert await stream.__anext__() == "a:two"
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()


async def test_stream_exception_releases_scope() -> None:
    app = make_app()
    log: list[str] = []

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", log))}

    async with app.app_context():
        stream = await stream_template_string(
            "{{ access.read('first') }}{{ 1 / 0 }}{{ access.read('never') }}"
        )
        assert await stream.__anext__() == "a:first"
        with pytest.raises(ZeroDivisionError):
            await stream.__anext__()
        # After a failed stream, further iteration fails clearly.
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()
    assert log == ["read:a:first", "close:a"]


async def test_two_accounts_interleaved_streams_are_isolated() -> None:
    log: list[str] = []

    def build(account: str) -> Quart:
        app = Quart(__name__)

        @app.context_processor
        async def provide() -> dict:
            return {"access": render_scoped(AccountAccess(account, log))}

        return app

    app_a = build("account-a")
    app_b = build("account-b")

    async def render(app: Quart, key: str) -> str:
        async with app.app_context():
            stream = await stream_template_string("{{ access.read(k) }}", k=key)
            return "".join([chunk async for chunk in stream])

    a1, b1, a2, b2 = await asyncio.gather(
        render(app_a, "one"),
        render(app_b, "two"),
        render(app_a, "three"),
        render(app_b, "four"),
    )
    assert (a1, b1, a2, b2) == (
        "account-a:one",
        "account-b:two",
        "account-a:three",
        "account-b:four",
    )
    reads = sorted(entry for entry in log if entry.startswith("read:"))
    assert reads == [
        "read:account-a:one",
        "read:account-a:three",
        "read:account-b:four",
        "read:account-b:two",
    ]
    closes = sorted(entry for entry in log if entry.startswith("close:"))
    assert closes == [
        "close:account-a",
        "close:account-a",
        "close:account-b",
        "close:account-b",
    ]


async def test_two_accounts_interleaved_in_one_task() -> None:
    log: list[str] = []

    def build(account: str) -> Quart:
        app = Quart(__name__)

        @app.context_processor
        async def provide() -> dict:
            return {"access": render_scoped(AccountAccess(account, log))}

        return app

    app_a = build("account-a")
    app_b = build("account-b")

    context_a = app_a.app_context()
    context_b = app_b.app_context()
    await context_a.push()
    stream_a = await stream_template_string(
        "{{ access.read('a1') }}|{{ access.read('a2') }}"
    )
    await context_b.push()
    stream_b = await stream_template_string(
        "{{ access.read('b1') }}|{{ access.read('b2') }}"
    )
    try:
        assert await stream_a.__anext__() == "account-a:a1"
        assert await stream_b.__anext__() == "account-b:b1"
        # Advance A again; it must still use A's object, not B's.
        assert await stream_a.__anext__() == "|"
        assert await stream_a.__anext__() == "account-a:a2"
        assert await stream_b.__anext__() == "|"
        assert await stream_b.__anext__() == "account-b:b2"
        with pytest.raises(StopAsyncIteration):
            await stream_a.__anext__()
        with pytest.raises(StopAsyncIteration):
            await stream_b.__anext__()
    finally:
        await stream_a.aclose()
        await stream_b.aclose()
        await context_b.pop()
        await context_a.pop()
    assert [entry for entry in log if entry.startswith("close:")] == [
        "close:account-a",
        "close:account-b",
    ]


async def test_two_accounts_release_order_when_interleaved(tmp_path: Path) -> None:
    log: list[str] = []
    (tmp_path / "stream.html").write_text(
        "{{ access.read('one') }}|{{ access.read('two') }}"
    )

    def build(account: str) -> Quart:
        app = Quart(__name__, template_folder=str(tmp_path))

        @app.context_processor
        async def provide() -> dict:
            return {"access": render_scoped(AccountAccess(account, log))}

        @app.route("/")
        async def index() -> ResponseReturnValue:
            return Response(await stream_template("stream.html"))

        return app

    client_a = build("account-a").test_client()
    client_b = build("account-b").test_client()
    body_a = await client_a.get("/")
    body_b = await client_b.get("/")
    # Both responses are open at once. Each account keeps its own object
    # and finishing A must not release or consume B's object.
    assert await body_a.get_data(as_text=True) == (
        "account-a:one|account-a:two"
    )
    assert await body_b.get_data(as_text=True) == (
        "account-b:one|account-b:two"
    )
    assert [entry for entry in log if entry.startswith("read:")] == [
        "read:account-a:one",
        "read:account-a:two",
        "read:account-b:one",
        "read:account-b:two",
    ]
    assert [entry for entry in log if entry.startswith("close:")] == [
        "close:account-a",
        "close:account-b",
    ]


async def test_two_account_interleaved_manual_iteration() -> None:
    log: list[str] = []
    streams = []
    for account in ("account-a", "account-b"):
        app = Quart(__name__)

        def provide(account: str = account):
            async def inner() -> dict:
                return {"access": render_scoped(AccountAccess(account, log))}

            return inner

        app.context_processor(provide())

        context = app.app_context()
        await context.push()
        stream = await stream_template_string("{{ access.read(tag) }}", tag=account)
        streams.append((context, stream))

    # Interleave: one chunk from each account before either finishes.
    assert await streams[0][1].__anext__() == "account-a:account-a"
    assert await streams[1][1].__anext__() == "account-b:account-b"
    await streams[0][1].aclose()
    await streams[1][1].aclose()
    for context, _ in reversed(streams):
        await context.pop()
    assert log == [
        "read:account-a:account-a",
        "read:account-b:account-b",
        "close:account-a",
        "close:account-b",
    ]


async def test_render_can_be_denied_before_template_executes() -> None:
    app = make_app()
    log: list[str] = []

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", log))}

    def approved(template: object, context: dict) -> bool:
        return False

    app.render_scope_checker = approved  # type: ignore[assignment]
    async with app.app_context():
        with pytest.raises(RenderScopeDeniedError):
            await render_template_string("{{ access.read('x') }}")
    assert log == ["close:a"]


async def test_render_denied_for_stream_before_first_chunk() -> None:
    app = make_app()
    log: list[str] = []

    @app.context_processor
    async def provide() -> dict:
        return {"access": render_scoped(AccountAccess("a", log))}

    async def denied(template: object, context: dict) -> bool:
        return False

    app.render_scope_checker = denied
    async with app.app_context():
        with pytest.raises(RenderScopeDeniedError):
            await stream_template_string("{{ access.read('x') }}")
    assert log == ["close:a"]


async def test_plain_values_and_unenabled_templates_unchanged() -> None:
    app = Quart(__name__)

    @app.context_processor
    async def provide() -> dict:
        return {"plain": "value", "obj": AccountAccess("plain", [])}

    async with app.app_context():
        rendered = await render_template_string("{{ plain }}{{ obj.read('x') }}")
        stream = await stream_template_string("{{ plain }}{{ obj.read('x') }}")
        streamed = "".join([chunk async for chunk in stream])
    assert rendered == "valueplain:x"
    assert streamed == "valueplain:x"


async def test_two_accounts_through_http_response_stream(tmp_path: Path) -> None:
    log: list[str] = []

    def build(account: str) -> Quart:
        app = Quart(__name__, template_folder=str(tmp_path))

        @app.context_processor
        async def provide() -> dict:
            return {"access": render_scoped(AccountAccess(account, log))}

        @app.route("/")
        async def index() -> ResponseReturnValue:
            return Response(await stream_template("stream.html"))

        return app

    (tmp_path / "stream.html").write_text(
        "{{ access.read('one') }}|{{ access.read('two') }}"
    )

    app_a = build("account-a")
    app_b = build("account-b")
    client_a = app_a.test_client()
    client_b = app_b.test_client()
    response_a = await client_a.get("/")
    response_b = await client_b.get("/")
    assert (await response_a.get_data(as_text=True)) == "account-a:one|account-a:two"
    assert (await response_b.get_data(as_text=True)) == "account-b:one|account-b:two"
    assert [entry for entry in log if entry.startswith("close:")] == [
        "close:account-a",
        "close:account-b",
    ]


async def test_blueprint_scoped_context_processor() -> None:
    app = Quart(__name__)
    blueprint = Blueprint("bp", __name__)
    log: list[str] = []

    @blueprint.context_processor
    async def provide() -> dict:
        return {"bp_access": render_scoped(AccountAccess("bp", log))}

    @blueprint.route("/")
    async def index() -> str:
        return ""

    app.register_blueprint(blueprint)
    async with app.app_context():
        with pytest.raises(Exception) as info:
            # A blueprint scoped value is not present for app renders.
            await render_template_string("{{ bp_access.read('x') }}")
        assert "undefined" in str(info.value).lower()
