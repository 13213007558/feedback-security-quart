"""Example: audited sensitive-file routes in a Quart application.

Run with::

    .venv/bin/python examples/delivery_audit_example.py

Only the two ``/files/...`` routes are audited; the health check route
is not.  Swap :class:`InMemoryAuditStore` for an ``AuditStore`` backed
by your database or audit service in a real application.
"""

from __future__ import annotations

from pathlib import Path

from quart import Quart

from delivery_audit import approved_delivery
from delivery_audit import audited_response
from delivery_audit import begin_delivery
from delivery_audit import InMemoryAuditStore
from delivery_audit import open_delivery

app = Quart(__name__)
audit_store = InMemoryAuditStore()

SECRET_DIR = Path(__file__).resolve().parent / "secret_files"


@app.get("/health")
async def health() -> dict:
    # Unaudited route: no audit record is created.
    return {"status": "ok"}


@app.get("/files/<path:name>")
async def download_file(name: str):
    # One-call helper for a file on disk.  If the "approved" audit
    # write fails this raises before any byte is sent.
    target = (SECRET_DIR / name).resolve()
    if not str(target).startswith(str(SECRET_DIR.resolve())):
        return "forbidden", 403
    return await approved_delivery(
        audit_store,
        target,
        requester="anonymous",
        meta={"route": "download_file"},
    )


@app.get("/exports/<report_id>")
async def download_report(report_id: str):
    # Lower-level API for a body produced on the fly or fetched remotely.
    record = await open_delivery(
        audit_store,
        file_ref=f"report:{report_id}",
        requester="anonymous",
        meta={"kind": "generated-report"},
    )
    await begin_delivery(record, audit_store)

    async def body():
        for chunk in (b"report,", b"line\n", b"done\n"):
            yield chunk

    return audited_response(record, body(), audit_store)


@app.get("/_audit")
async def dump_audit():
    return {
        record.audit_id: {
            "file_ref": record.file_ref,
            "state": record.state.value,
            "bytes_delivered": record.bytes_delivered,
            "error": record.error,
        }
        for record in audit_store.all()
    }


if __name__ == "__main__":
    SECRET_DIR.mkdir(exist_ok=True)
    (SECRET_DIR / "sample.txt").write_bytes(b"sensitive contents\n" * 100)
    app.run()
