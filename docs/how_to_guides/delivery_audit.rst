.. _delivery_audit:

Auditing sensitive file delivery
================================

When a route serves sensitive files, every delivery should be
traceable to an audit record even if the response fails before the
first byte is sent or the client cancels halfway through.  The
``delivery_audit`` package provides a small, opt-in flow built on the
audit lifecycle::

    approved -> started -> completed
                        \\-> interrupted

The states mean:

* ``approved`` — the request was authorised and the audit record was
  written before the file was opened.
* ``started`` — the record was moved to ``started`` in the view, before
  Quart emits the response headers, so the first chunk can only be
  sent once this write is durable.
* ``completed`` — the body was exhausted and every byte was handed to
  the ASGI server.
* ``interrupted`` — the delivery ended early (client disconnect, source
  error, cancellation).  The record keeps the number of bytes already
  delivered, so partial deliveries remain checkable.

Only routes that call the audit functions are affected; every other
route behaves exactly as before.

Storage interface
-----------------

The application owns persistence by implementing the
:class:`~delivery_audit.AuditStore` interface (``create``, ``get``,
``progress`` and compare-and-set ``transition``).  An
:class:`~delivery_audit.InMemoryAuditStore` is included for tests and
local development; it can also be told to fail at any stage to verify
the failure handling of a real store.

Serving a file from disk
------------------------

:func:`~delivery_audit.approved_delivery` performs the whole flow for a
file on disk:

.. code-block:: python

    from delivery_audit import InMemoryAuditStore, approved_delivery

    audit_store = InMemoryAuditStore()

    @app.get("/files/<path:name>")
    async def download_file(name):
        return await approved_delivery(
            audit_store,
            f"/var/secure/{name}",
            requester=current_user.id,
            meta={"route": "download_file"},
        )

The response carries an ``X-Audit-Id`` header whose value matches
``record.audit_id`` in the store.

Generated or remote bodies
--------------------------

For bodies produced on the fly or fetched from another service, use
the three primitives explicitly.  ``begin_delivery`` must run before
the response is returned so a failed write aborts with no bytes sent:

.. code-block:: python

    from delivery_audit import (
        audited_response, begin_delivery, open_delivery,
    )

    @app.get("/exports/<report_id>")
    async def export(report_id):
        record = await open_delivery(
            audit_store, f"report:{report_id}", requester=current_user.id
        )
        await begin_delivery(record, audit_store)

        async def body():
            async for chunk in report_stream(report_id):
                yield chunk

        return audited_response(record, body(), audit_store)

Failure semantics
-----------------

* A failed ``approved`` or ``started`` write raises out of the view.
  Quart returns an error response and the file body is never iterated,
  so no byte leaves the process.
* After the first chunk has been yielded, an audit failure must not
  stop an in-flight download.  Progress and completion failures are
  passed to an error sink (a logging call by default) and the transfer
  continues.  A failed completion write leaves the record in
  ``started`` with the delivered byte count rather than falsely
  recording success.
* A disconnect or body error after the first chunk records
  ``interrupted`` plus partial bytes.  Terminal transitions are
  compare-and-set: re-entrant cleanup (for example generator close
  racing normal completion) is idempotent and can never create a
  second ``completed`` record or overwrite one.

Testing store failures
----------------------

The reference store accepts operation names (``create``, ``progress``,
``transition``) or operation/state pairs such as
``transition:started``:

.. code-block:: python

    audit_store.fail_next("create")          # next approval fails once
    audit_store.fail_always("progress")      # every progress write fails
    audit_store.recover()                    # clear injected failures
