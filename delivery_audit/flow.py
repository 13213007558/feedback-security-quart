"""Quart integration for audited sensitive-file delivery.

The flow is opt-in per route: a route calls :func:`open_delivery` and
:func:`begin_delivery` (or the one-call :func:`approved_delivery`) and
returns the response from :func:`audited_response`.  Routes that never
call these functions keep ordinary, unaudited behaviour.

Safety rules implemented here:

* The ``approved`` record is written before the file is opened.  If
  that write fails the route raises and Quart's error handling answers
  without ever touching the file, so no byte leaves the process.
* The ``started`` write happens before the response is returned, i.e.
  before Quart emits status line and headers.  A failure there likewise
  aborts the route with zero bytes sent.  The streaming generator
  repeats the transition as an idempotent defense-in-depth gate.
* After the first chunk has been yielded, audit failures must not
  block an in-flight delivery: they are reported through the
  configured error sink and the transfer continues.
* Interruption after the first chunk leaves a checkable partial
  delivery (``interrupted`` plus the delivered-byte counter).
* Terminal transitions are compare-and-set, so generator close racing
  normal completion (or any re-entrant cleanup) can never produce a
  second success record or overwrite a completed delivery.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from collections.abc import Callable
from pathlib import Path
from typing import Any

from quart import Response

from .models import AuditRecord
from .models import AuditState
from .store import AuditStore
from .store import AuditStoreError
from .store import describe_error

logger = logging.getLogger("delivery_audit")

ErrorSink = Callable[[AuditRecord, AuditStoreError], None]
ChunkSize = 64 * 1024


def _default_error_sink(record: AuditRecord, error: AuditStoreError) -> None:
    logger.error(
        "delivery audit write failed audit_id=%s state=%s: %s",
        record.audit_id,
        record.state.value,
        error,
    )


async def open_delivery(
    store: AuditStore,
    file_ref: str,
    *,
    requester: str | None = None,
    bytes_total: int | None = None,
    meta: dict[str, Any] | None = None,
) -> AuditRecord:
    """Persist the ``approved`` record for a new delivery.

    A failure here propagates to the route, which prevents the file
    from being opened or sent at all.  Follow with
    :func:`begin_delivery` once the body is ready, or use
    :func:`approved_delivery` for the common file-on-disk case.
    """

    record = AuditRecord(
        file_ref=file_ref,
        requester=requester,
        state=AuditState.APPROVED,
        bytes_total=bytes_total,
        meta=dict(meta or {}),
    )
    return await store.create(record)


async def begin_delivery(
    record: AuditRecord,
    store: AuditStore,
) -> AuditRecord:
    """Move an approved record to ``started`` before any byte is sent.

    This runs in the route, before the response object is returned, so
    a store failure aborts the request with no response headers and no
    file data emitted.  The transition is compare-and-set on
    ``approved``; calling it more than once is a harmless no-op that
    returns the authoritative state.
    """

    stored_state = await store.transition(
        record.audit_id, AuditState.STARTED, expected=AuditState.APPROVED
    )
    if stored_state == AuditState.STARTED:
        record.state = AuditState.STARTED
    return record


def audited_response(
    record: AuditRecord,
    body: AsyncIterable[bytes],
    store: AuditStore,
    *,
    error_sink: ErrorSink | None = None,
    status: int = 200,
    headers: dict | None = None,
    mimetype: str | None = None,
) -> Response:
    """Wrap an async byte body in an audited streaming response.

    The record should already be ``started`` (see
    :func:`begin_delivery`).  The response carries an ``X-Audit-Id``
    header so the client and the audit log can be correlated.
    """

    on_error = error_sink or _default_error_sink
    response_headers = dict(headers or {})
    response_headers["X-Audit-Id"] = record.audit_id
    return Response(
        _audited_stream(record, body, store, on_error),
        status=status,
        headers=response_headers,
        mimetype=mimetype,
    )


async def approved_delivery(
    store: AuditStore,
    file_path: str | Path,
    *,
    requester: str | None = None,
    meta: dict[str, Any] | None = None,
    error_sink: ErrorSink | None = None,
    chunk_size: int = ChunkSize,
    as_attachment: bool = True,
) -> Response:
    """One-call helper: approve, start and audit a file on disk.

    The file is only opened after the ``approved`` audit write
    succeeds, and the ``started`` write completes before the response
    is returned.  Use :func:`open_delivery` + :func:`begin_delivery` +
    :func:`audited_response` for generated or remote bodies.
    """

    path = Path(file_path)
    record = await open_delivery(
        store,
        str(path),
        requester=requester,
        bytes_total=path.stat().st_size,
        meta=meta,
    )

    async def _file_body() -> AsyncIterator[bytes]:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    await begin_delivery(record, store)
    response = audited_response(
        record,
        _file_body(),
        store,
        error_sink=error_sink,
    )
    if as_attachment:
        response.headers.add(
            "Content-Disposition", "attachment", filename=path.name
        )
    return response


async def _audited_stream(
    record: AuditRecord,
    body: AsyncIterable[bytes],
    store: AuditStore,
    on_error: ErrorSink,
) -> AsyncIterator[bytes]:
    audit_id = record.audit_id
    bytes_delivered = 0
    first_chunk_sent = False

    async def _mark_interrupted(error: BaseException) -> None:
        """Re-entrant safe: no-op once the delivery is terminal.

        Both ``approved`` and ``started`` records may reach this point
        (the route-level ``started`` write can already have happened),
        so no expected state is pinned; the store rejects backward
        transitions and treats an already-terminal record as a no-op.
        """

        try:
            stored_state = await store.transition(
                audit_id,
                AuditState.INTERRUPTED,
                error=describe_error(error),
            )
        except AuditStoreError as audit_error:
            on_error(record, audit_error)
            return
        if stored_state == AuditState.INTERRUPTED:
            record.state = AuditState.INTERRUPTED
            record.error = describe_error(error)

    try:
        async for chunk in body:
            if not chunk:
                continue

            if not first_chunk_sent:
                # Defense-in-depth gate: begin_delivery() normally made
                # this write in the route.  Repeating it is idempotent
                # because the store treats same-state and terminal
                # records as no-ops; if the store is unavailable here
                # headers may already be on the wire, so report and
                # continue rather than tearing down the response.
                try:
                    stored_state = await store.transition(
                        audit_id, AuditState.STARTED
                    )
                except AuditStoreError as audit_error:
                    on_error(record, audit_error)
                else:
                    if stored_state == AuditState.STARTED:
                        record.state = AuditState.STARTED
                first_chunk_sent = True

            bytes_delivered += len(chunk)
            record.bytes_delivered = bytes_delivered
            try:
                await store.progress(audit_id, bytes_delivered)
            except AuditStoreError as error:
                # The transfer has begun; an audit hiccup must not stop
                # a file the client is already receiving.
                on_error(record, error)

            yield chunk

        # Normal end of body.  A failed completion write leaves the
        # delivery checkable as "started + partial bytes" rather than
        # silently claiming success; cleanup cannot create a second
        # success record thanks to the compare-and-set.
        try:
            stored_state = await store.transition(
                audit_id, AuditState.COMPLETED, expected=AuditState.STARTED
            )
        except AuditStoreError as error:
            on_error(record, error)
        else:
            if stored_state == AuditState.COMPLETED:
                record.state = AuditState.COMPLETED
    except BaseException as error:
        # Whether or not a chunk was yielded, try to leave a checkable
        # interrupted record.  When nothing left the process the record
        # keeps zero delivered bytes; if that write also fails, the
        # "approved" record is still the proof that no file data was
        # sent.  Re-raise either way so a generator closed mid-flight
        # surfaces the cancellation to the ASGI server.
        await _mark_interrupted(error)
        raise
