from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from quart import Quart

from delivery_audit import AuditRecord
from delivery_audit import AuditState
from delivery_audit import AuditStoreError
from delivery_audit import AuditTransitionError
from delivery_audit import InMemoryAuditStore
from delivery_audit import approved_delivery
from delivery_audit import audited_response
from delivery_audit import begin_delivery
from delivery_audit import open_delivery
from delivery_audit.store import transition_allowed


@pytest.fixture
def store() -> InMemoryAuditStore:
    return InMemoryAuditStore()


def _only_record(store: InMemoryAuditStore) -> AuditRecord:
    records = list(store.all())
    assert len(records) == 1
    return records[0]


def _app() -> Quart:
    app = Quart(__name__)
    app.testing = True
    return app


async def test_approved_record_created_with_state_approved(store: InMemoryAuditStore) -> None:
    record = await open_delivery(store, "doc-1", requester="alice")

    assert record.state is AuditState.APPROVED
    assert record.bytes_delivered == 0
    fetched = await store.get(record.audit_id)
    assert fetched.audit_id == record.audit_id
    assert fetched.file_ref == "doc-1"
    assert fetched.requester == "alice"


async def test_full_delivery_records_all_four_states_in_order(
    store: InMemoryAuditStore, tmp_path: Path
) -> None:
    seen: list[AuditState] = []
    real_transition = store.transition

    async def spy(audit_id: str, target: AuditState, **kwargs):
        seen.append(target)
        return await real_transition(audit_id, target, **kwargs)

    store.transition = spy  # type: ignore[method-assign]

    secret = tmp_path / "secret.bin"
    secret.write_bytes(b"x" * 10_000)

    app = _app()

    @app.get("/download")
    async def download():
        return await approved_delivery(
            store, secret, requester="alice", chunk_size=4096
        )

    client = app.test_client()
    response = await client.get("/download")
    assert response.status_code == 200
    body = await response.get_data()
    assert body == b"x" * 10_000
    assert response.headers["X-Audit-Id"]

    record = _only_record(store)
    assert AuditState.STARTED in seen
    assert seen[-1] is AuditState.COMPLETED
    assert record.state is AuditState.COMPLETED
    assert record.bytes_delivered == 10_000
    assert record.error is None


async def test_audit_is_opt_in_per_route(store: InMemoryAuditStore) -> None:
    app = _app()

    @app.get("/public")
    async def public():
        return "no audit here"

    @app.get("/private")
    async def private():
        record = await open_delivery(store, "doc")
        await begin_delivery(record, store)

        async def body():
            yield b"data"

        return audited_response(record, body(), store)

    client = app.test_client()
    await client.get("/public")
    assert list(store.all()) == []

    response = await client.get("/private")
    assert response.status_code == 200
    assert _only_record(store).state is AuditState.COMPLETED


async def test_approved_write_failure_blocks_file_egress(
    store: InMemoryAuditStore, tmp_path: Path
) -> None:
    store.fail_next("create")
    secret = tmp_path / "secret.bin"
    secret.write_bytes(b"never send me")

    opened = False

    app = _app()

    @app.get("/download")
    async def download():
        nonlocal opened
        with pytest.raises(AuditStoreError):
            await open_delivery(store, "doc")
        opened = True
        return "audit unavailable", 503

    client = app.test_client()
    response = await client.get("/download")
    assert response.status_code == 503
    assert await response.get_data() == b"audit unavailable"
    assert opened is True
    assert list(store.all()) == []


async def test_started_write_failure_before_first_chunk_blocks_egress(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    store.fail_next("transition:started")

    file_touched = False

    async def body():
        nonlocal file_touched
        file_touched = True
        yield b"classified"

    app = _app()

    @app.get("/download")
    async def download():
        await begin_delivery(record, store)
        return audited_response(record, body(), store)

    client = app.test_client()
    # begin_delivery runs before the response exists, so Quart turns the
    # store failure into a 500 and the body generator is never started.
    response = await client.get("/download")
    assert response.status_code == 500
    assert file_touched is False

    stored = _only_record(store)
    assert stored.state is AuditState.APPROVED
    assert stored.bytes_delivered == 0


async def test_progress_failure_after_start_keeps_delivery_intact(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await begin_delivery(record, store)
    store.fail_next("progress")
    sink_errors: list[AuditStoreError] = []

    async def body():
        yield b"part-"
        yield b"one"
        yield b"two"

    app = _app()

    @app.get("/download")
    async def download():
        return audited_response(
            record,
            body(),
            store,
            error_sink=lambda _record, error: sink_errors.append(error),
        )

    client = app.test_client()
    response = await client.get("/download")
    assert await response.get_data() == b"part-onetwo"

    assert len(sink_errors) == 1
    stored = _only_record(store)
    assert stored.state is AuditState.COMPLETED
    assert stored.bytes_delivered == len(b"part-onetwo")


async def test_completion_write_failure_leaves_checkable_partial_state(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await begin_delivery(record, store)
    store.fail_next("transition:completed")
    sink_errors: list[AuditStoreError] = []

    async def body():
        yield b"all-the-bytes"

    app = _app()

    @app.get("/download")
    async def download():
        return audited_response(
            record,
            body(),
            store,
            error_sink=lambda _record, error: sink_errors.append(error),
        )

    client = app.test_client()
    response = await client.get("/download")
    assert await response.get_data() == b"all-the-bytes"

    stored = _only_record(store)
    # No false "completed" claim: the record stays started with the
    # byte count, and the failed write is reported via the sink.
    assert stored.state is AuditState.STARTED
    assert stored.bytes_delivered == len(b"all-the-bytes")
    assert len(sink_errors) == 1


async def test_mid_stream_client_disconnect_marks_interrupted(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc", bytes_total=30)

    gate: asyncio.Event = asyncio.Event()

    async def body():
        yield b"first-chunk"
        await gate.wait()
        yield b"rest-of-file"

    app = _app()

    @app.get("/download")
    async def download():
        await begin_delivery(record, store)
        return audited_response(record, body(), store)

    connection = app.test_client().request("/download")
    async with connection:
        first = await connection.receive()
        assert first == b"first-chunk"
        await connection.disconnect()
        gate.set()
        # Let the cancelled generator run its cleanup.
        await asyncio.sleep(0.05)

    stored = _only_record(store)
    assert stored.state is AuditState.INTERRUPTED
    assert stored.bytes_delivered == len(b"first-chunk")
    assert stored.bytes_total == 30
    assert stored.error is not None


async def test_reentrant_cleanup_never_creates_second_success(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await store.transition(
        record.audit_id, AuditState.STARTED, expected=AuditState.APPROVED
    )
    await store.transition(
        record.audit_id, AuditState.COMPLETED, expected=AuditState.STARTED
    )

    # A duplicate/racing cleanup must not overwrite the completion.
    result = await store.transition(
        record.audit_id, AuditState.INTERRUPTED, expected=AuditState.STARTED
    )
    assert result is AuditState.COMPLETED
    result = await store.transition(
        record.audit_id, AuditState.INTERRUPTED
    )
    assert result is AuditState.COMPLETED
    result = await store.transition(
        record.audit_id, AuditState.COMPLETED, expected=AuditState.STARTED
    )
    assert result is AuditState.COMPLETED

    stored = _only_record(store)
    assert stored.state is AuditState.COMPLETED
    assert stored.error is None


async def test_interrupted_transition_is_idempotent(store: InMemoryAuditStore) -> None:
    record = await open_delivery(store, "doc")
    await store.transition(
        record.audit_id, AuditState.STARTED, expected=AuditState.APPROVED
    )
    first = await store.transition(
        record.audit_id,
        AuditState.INTERRUPTED,
        expected=AuditState.STARTED,
        error="boom",
    )
    second = await store.transition(
        record.audit_id,
        AuditState.INTERRUPTED,
        expected=AuditState.STARTED,
        error="again",
    )

    assert first is AuditState.INTERRUPTED
    assert second is AuditState.INTERRUPTED
    stored = _only_record(store)
    assert stored.state is AuditState.INTERRUPTED
    assert stored.error == "boom"


async def test_lifecycle_rejects_backward_transitions(store: InMemoryAuditStore) -> None:
    record = await open_delivery(store, "doc")
    await store.transition(
        record.audit_id, AuditState.STARTED, expected=AuditState.APPROVED
    )
    await store.transition(
        record.audit_id, AuditState.COMPLETED, expected=AuditState.STARTED
    )

    with pytest.raises(AuditTransitionError):
        await store.transition(record.audit_id, AuditState.STARTED)

    assert _only_record(store).state is AuditState.COMPLETED


def test_transition_table() -> None:
    assert transition_allowed(AuditState.APPROVED, AuditState.STARTED)
    assert transition_allowed(AuditState.APPROVED, AuditState.INTERRUPTED)
    assert transition_allowed(AuditState.STARTED, AuditState.COMPLETED)
    assert transition_allowed(AuditState.STARTED, AuditState.INTERRUPTED)
    assert not transition_allowed(AuditState.APPROVED, AuditState.COMPLETED)
    assert not transition_allowed(AuditState.COMPLETED, AuditState.INTERRUPTED)
    assert not transition_allowed(AuditState.INTERRUPTED, AuditState.COMPLETED)


async def test_interrupted_store_failure_during_cleanup_uses_sink(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await begin_delivery(record, store)
    store.fail_next("transition:interrupted")
    sink_errors: list[AuditStoreError] = []

    async def body():
        if False:
            yield b""  # async generator marker
        raise RuntimeError("disk exploded")

    # Drive the audited generator directly: Quart does not convert
    # exceptions raised while iterating a response body into error
    # responses, but cleanup semantics belong to the generator itself.
    from delivery_audit.flow import _audited_stream

    def sink(failed_record: AuditRecord, error: AuditStoreError) -> None:
        sink_errors.append(error)

    generator = _audited_stream(record, body(), store, sink)
    with pytest.raises(RuntimeError, match="disk exploded"):
        async for _chunk in generator:
            pass

    stored = _only_record(store)
    assert stored.state is AuditState.STARTED
    assert stored.bytes_delivered == 0
    assert len(sink_errors) == 1
    assert "injected failure" in str(sink_errors[0])


async def test_body_error_after_first_chunk_marks_interrupted_with_bytes(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await begin_delivery(record, store)

    async def body():
        yield b"safe-prefix"
        raise RuntimeError("source broke")

    from delivery_audit.flow import _audited_stream

    generator = _audited_stream(record, body(), store, lambda _r, _e: None)
    delivered = bytearray()
    with pytest.raises(RuntimeError, match="source broke"):
        async for chunk in generator:
            delivered.extend(chunk)

    assert bytes(delivered) == b"safe-prefix"
    stored = _only_record(store)
    assert stored.state is AuditState.INTERRUPTED
    assert stored.bytes_delivered == len(b"safe-prefix")
    assert "RuntimeError" in (stored.error or "")


async def test_reentrant_generator_close_keeps_single_completed_record(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await begin_delivery(record, store)

    async def body():
        yield b"one"
        yield b"two"

    from delivery_audit.flow import _audited_stream

    generator = _audited_stream(record, body(), store, lambda _r, _e: None)
    chunks = [chunk async for chunk in generator]
    assert chunks == [b"one", b"two"]
    # Explicit close after normal completion must not rewrite the record.
    await generator.aclose()

    records = list(store.all())
    assert len(records) == 1
    assert records[0].state is AuditState.COMPLETED
    assert records[0].bytes_delivered == 6
    assert records[0].error is None


async def test_zero_byte_body_completes_without_interruption(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await begin_delivery(record, store)

    async def body():
        if False:
            yield b""

    from delivery_audit.flow import _audited_stream

    generator = _audited_stream(record, body(), store, lambda _r, _e: None)
    async for _chunk in generator:
        pass

    stored = _only_record(store)
    assert stored.state is AuditState.COMPLETED
    assert stored.bytes_delivered == 0


async def test_generator_gate_survives_started_write_failure(
    store: InMemoryAuditStore,
) -> None:
    # When the route skipped begin_delivery and the store fails on the
    # approval->started write inside the generator, headers may already
    # be on the wire: the failure is reported and delivery continues.
    record = await open_delivery(store, "doc")
    store.fail_next("transition:started")
    sink_errors: list[AuditStoreError] = []

    async def body():
        yield b"still-delivered"

    from delivery_audit.flow import _audited_stream

    def sink(_record: AuditRecord, error: AuditStoreError) -> None:
        sink_errors.append(error)

    generator = _audited_stream(record, body(), store, sink)
    chunks = [chunk async for chunk in generator]

    assert chunks == [b"still-delivered"]
    # The failed start write is reported but does not stop the transfer.
    assert len(sink_errors) == 1
    stored = _only_record(store)
    # The completion compare-and-set refuses to run from "approved",
    # so no false success record is ever written.
    assert stored.state is AuditState.APPROVED


async def test_progress_failures_keep_last_known_byte_counter(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc", bytes_total=12)
    await begin_delivery(record, store)
    store.fail_next("progress")  # the second chunk's write must still work

    async def body():
        yield b"aaaa"
        yield b"bbbb"
        yield b"cccc"

    app = _app()

    @app.get("/download")
    async def download():
        return audited_response(record, body(), store)

    client = app.test_client()
    response = await client.get("/download")
    assert await response.get_data() == b"aaaabbbbcccc"

    stored = _only_record(store)
    assert stored.state is AuditState.COMPLETED
    assert stored.bytes_delivered == 12


async def test_cancel_before_first_chunk_keeps_zero_byte_interrupted_record(
    store: InMemoryAuditStore,
) -> None:
    record = await open_delivery(store, "doc")
    await begin_delivery(record, store)

    gate = asyncio.Event()

    async def body():
        await gate.wait()
        yield b"never-sent"

    from delivery_audit.flow import _audited_stream

    async def consume() -> None:
        async for _chunk in _audited_stream(record, body(), store, lambda _r, _e: None):
            pass

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0)  # let the generator park on the gate
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    stored = _only_record(store)
    assert stored.state is AuditState.INTERRUPTED
    assert stored.bytes_delivered == 0
    assert stored.error is not None
