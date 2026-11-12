from __future__ import annotations

import asyncio

import pytest
from hypercorn.typing import ASGIReceiveEvent
from hypercorn.typing import ASGISendEvent

from quart import Quart
from quart import QuotaService
from quart import ServiceClosedError
from quart.signals import got_background_exception


def _service() -> QuotaService:
    # Two accounts, each allowing a single signing-service lease.
    return QuotaService({"a": 1, "b": 1})


async def test_quota_grants_in_fifo_order() -> None:
    service = _service()
    quota = service.account("a")
    order: list[str] = []
    first_acquired = asyncio.Event()

    async def worker(name: str, gate: asyncio.Event | None = None) -> None:
        async with service.lease("a"):
            order.append(f"{name}:start")
            if gate is not None:
                gate.set()
            await asyncio.sleep(0.02)
            order.append(f"{name}:end")

    task1 = asyncio.create_task(worker("one", first_acquired))
    await first_acquired.wait()
    assert quota.in_use == 1

    task2 = asyncio.create_task(worker("two"))
    task3 = asyncio.create_task(worker("three"))
    await asyncio.sleep(0)
    assert quota.waiting == 2

    await asyncio.gather(task1, task2, task3)
    assert order == [
        "one:start",
        "one:end",
        "two:start",
        "two:end",
        "three:start",
        "three:end",
    ]
    assert quota.in_use == 0
    assert quota.waiting == 0


async def test_account_quotas_are_independent() -> None:
    service = _service()
    progress = {"a": 0, "b": 0}
    gate = asyncio.Event()

    async def worker(account_id: str) -> None:
        async with service.lease(account_id):
            progress[account_id] += 1
            await gate.wait()

    task_a1 = asyncio.create_task(worker("a"))
    task_b1 = asyncio.create_task(worker("b"))
    await asyncio.sleep(0.02)
    assert progress == {"a": 1, "b": 1}

    task_a2 = asyncio.create_task(worker("a"))
    task_b2 = asyncio.create_task(worker("b"))
    await asyncio.sleep(0.02)
    assert service.account("a").waiting == 1
    assert service.account("b").waiting == 1
    assert progress == {"a": 1, "b": 1}

    gate.set()
    await asyncio.gather(task_a1, task_b1, task_a2, task_b2)
    assert progress == {"a": 2, "b": 2}
    assert service.account("a").in_use == 0
    assert service.account("b").in_use == 0


async def test_cancelled_waiter_never_takes_a_slot() -> None:
    service = _service()
    quota = service.account("a")
    holder_done = asyncio.Event()

    async def holder() -> None:
        async with service.lease("a"):
            await holder_done.wait()

    hold_task = asyncio.create_task(holder())
    await asyncio.sleep(0)

    async def waiter() -> None:
        async with service.lease("a"):
            pytest.fail("waiter must not acquire the slot")

    waiting_task = asyncio.create_task(waiter())
    await asyncio.sleep(0)
    assert quota.waiting == 1

    waiting_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting_task
    assert quota.waiting == 0
    assert quota.in_use == 1

    holder_done.set()
    await hold_task

    async def replacement() -> None:
        async with service.lease("a"):
            pass

    next_task = asyncio.create_task(replacement())
    # If the cancelled waiter leaked a slot this hangs until the timeout.
    await asyncio.wait_for(next_task, timeout=1)
    assert quota.in_use == 0


async def test_close_revokes_waiters_and_rejects_new_work() -> None:
    service = _service()
    quota = service.account("a")
    release_holder = asyncio.Event()
    holder_started = asyncio.Event()

    async def holder() -> None:
        async with service.lease("a"):
            holder_started.set()
            await release_holder.wait()

    async def waiter() -> None:
        async with service.lease("a"):
            pytest.fail("waiter must not run after close")

    hold_task = asyncio.create_task(holder())
    await holder_started.wait()
    waiting_task = asyncio.create_task(waiter())
    await asyncio.sleep(0)

    service.close()
    assert service.closed and quota.closed
    with pytest.raises(ServiceClosedError):
        await waiting_task
    with pytest.raises(ServiceClosedError):
        await service.acquire("a")
    with pytest.raises(ServiceClosedError):
        service.add_account("c", 1)

    release_holder.set()
    await hold_task
    assert quota.in_use == 0


async def test_release_after_exception_keeps_counter_balanced() -> None:
    service = QuotaService({"a": 2})
    quota = service.account("a")

    async def flaky() -> None:
        async with service.lease("a"):
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await flaky()
    assert quota.in_use == 0

    lease = await service.acquire("a")
    assert quota.in_use == 1
    await lease.release()
    await lease.release()
    assert quota.in_use == 0


async def test_invalid_limit_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        QuotaService({"a": 0})


# --- Quart background task integration ----------------------------------


async def test_background_tasks_wait_for_account_quota() -> None:
    app = Quart(__name__)
    service = QuotaService({"a": 1}, app=app)
    quota = service.account("a")
    started: list[int] = []
    concurrent = 0
    max_concurrent = 0
    gate = asyncio.Event()

    async def job(job_id: int) -> None:
        nonlocal concurrent, max_concurrent
        # The framework already holds the account lease while this body
        # runs; the live counter therefore must never exceed the limit.
        assert quota.in_use == 1
        started.append(job_id)
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        await gate.wait()
        concurrent -= 1

    @app.route("/<int:job_id>")
    async def index(job_id: int) -> str:
        app.add_background_task(job, job_id, account_id="a")
        return ""

    async with app.test_app() as test_app:
        client = test_app.test_client()
        await client.get("/1")
        await client.get("/2")
        await asyncio.sleep(0.05)
        assert started == [1]
        assert max_concurrent == 1
        assert quota.waiting == 1
        gate.set()

    assert started == [1, 2]
    assert quota.in_use == 0
    assert quota.waiting == 0


async def test_runner_exception_releases_its_lease() -> None:
    app = Quart(__name__)
    service = QuotaService({"a": 1}, app=app)
    quota = service.account("a")
    proceed = asyncio.Event()
    failures: list[Exception] = []
    ok_task_ran = False

    async def _record(sender: Quart, exception: Exception) -> None:
        failures.append(exception)

    got_background_exception.connect(_record, app)

    async def broken() -> None:
        await proceed.wait()
        raise RuntimeError("signing service failed")

    async def ok() -> None:
        nonlocal ok_task_ran
        ok_task_ran = True

    @app.route("/")
    async def index() -> str:
        app.add_background_task(broken, account_id="a")
        return ""

    try:
        async with app.test_app() as test_app:
            client = test_app.test_client()
            await client.get("/")
            await asyncio.sleep(0.02)
            assert quota.in_use == 1
            proceed.set()
            # Give the broken task time to finish and release the lease.
            await asyncio.sleep(0.05)
            assert quota.in_use == 0
            app.add_background_task(ok, account_id="a")

        assert failures and isinstance(failures[0], RuntimeError)
        assert ok_task_ran
    finally:
        got_background_exception.disconnect(_record, app)


async def test_shutdown_revokes_waiting_background_tasks() -> None:
    app = Quart(__name__)
    service = QuotaService({"a": 1}, app=app)
    quota = service.account("a")
    holder_started = asyncio.Event()
    waiter_ran = False

    async def holder() -> None:
        holder_started.set()
        # Only release the running slot after shutdown has revoked the
        # waiter, proving a freed slot is never re-granted once closed.
        while not service.closed:
            await asyncio.sleep(0.01)

    async def waiter() -> None:
        nonlocal waiter_ran
        waiter_ran = True

    @app.before_serving
    async def startup() -> None:
        app.add_background_task(holder, account_id="a")
        await holder_started.wait()
        app.add_background_task(waiter, account_id="a")
        await asyncio.sleep(0)
        assert quota.waiting == 1

    async with app.test_app():
        pass

    assert waiter_ran is False
    assert quota.in_use == 0
    assert quota.waiting == 0
    assert service.closed


async def test_shutdown_rejects_new_background_tasks() -> None:
    app = Quart(__name__)
    QuotaService({"a": 1}, app=app)

    @app.after_serving
    async def after_serving() -> None:
        with pytest.raises(ServiceClosedError):
            app.add_background_task(lambda: None, account_id="a")

    async with app.test_app():
        pass


async def test_request_cancellation_keeps_admitted_task() -> None:
    app = Quart(__name__)
    QuotaService({"a": 1}, app=app)
    task_completed = asyncio.Event()
    admitted = asyncio.Event()

    async def slow_job() -> None:
        admitted.set()
        await asyncio.sleep(0.05)
        task_completed.set()

    @app.route("/")
    async def index() -> str:
        app.add_background_task(slow_job, account_id="a")
        # Give the background task time to be admitted while the request
        # is still in flight, then let the client disconnect.
        await admitted.wait()
        await asyncio.sleep(0.1)
        return ""

    scope = {
        "type": "http",
        "asgi": {},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"quart")],
        "client": ("127.0.0.1", 80),
        "server": None,
        "extensions": {},
        "state": {},
    }
    send_queue: asyncio.Queue = asyncio.Queue()
    send_queue.put_nowait(
        {"type": "http.request", "body": b"", "more_body": False}
    )

    async def receive() -> ASGIReceiveEvent:
        return await send_queue.get()

    async def send(message: ASGISendEvent) -> None:
        pass

    async with app.test_app():
        request_task = asyncio.ensure_future(app(scope, receive, send))
        await admitted.wait()
        # The client goes away before the response is sent.
        await send_queue.put({"type": "http.disconnect"})
        await asyncio.wait_for(request_task, timeout=2)
        # The admitted background task belongs to the app and survives
        # the request cancellation; it keeps its lease until it finishes.
        await asyncio.wait_for(task_completed.wait(), timeout=2)
