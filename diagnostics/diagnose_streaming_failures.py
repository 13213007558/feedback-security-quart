"""Controlled local diagnosis of three overlapping Quart download failures.

Failures under test (all injected deterministically, no network / port):

  A. Permission revocation   -> view/generator raises ``PermissionError``
                                (authorization withdrawn mid-download).
  B. Generator failure       -> the streaming body generator raises an
                                unrelated ``RuntimeError`` while iterating.
  C. Teardown failure        -> a ``teardown_request`` callback raises
                                ``IOError`` while the request context exits.

The harness drives the real ``Quart`` ASGI app in-process, records every
outbound ASGI event (so the bytes already sent are hard evidence), captures
app log records, and reports which exception is ultimately visible at the ASGI
boundary and how exceptions chain/overwrite each other.

Run::

    .venv/bin/python diagnostics/diagnose_streaming_failures.py
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hypercorn.typing import ASGIReceiveCallable, ASGISendCallable  # noqa: E402

from quart import Quart  # noqa: E402
from quart.asgi import ASGIHTTPConnection  # noqa: E402
from quart.ctx import AppContext  # noqa: E402
from quart.signals import got_serving_exception  # noqa: E402


# --------------------------------------------------------------------------- #
# Domain errors
# --------------------------------------------------------------------------- #
class PermissionRevokedError(PermissionError):
    """Authorization for this download was revoked before it completed."""


class GeneratorBoomError(RuntimeError):
    """The download generator itself failed."""


class TeardownBoomError(IOError):
    """A request teardown (cleanup) callback failed."""


# --------------------------------------------------------------------------- #
# Ordered trace + log capture
# --------------------------------------------------------------------------- #
@dataclass
class Trace:
    seq: int = 0
    events: list[tuple[int, str, str]] = field(default_factory=list)

    def add(self, phase: str, detail: str) -> None:
        self.seq += 1
        self.events.append((self.seq, phase, detail))

    def render(self) -> str:
        width = max(len(phase) for _, phase, _ in self.events)
        return "\n".join(
            f"  {n:02d}. {phase.ljust(width)} | {detail}" for n, phase, detail in self.events
        )


class ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


# --------------------------------------------------------------------------- #
# In-process ASGI probe: records raw send events and the boundary exception
# --------------------------------------------------------------------------- #
class LegacyASGIHTTPConnection(ASGIHTTPConnection):
    """Exact pre-fix sending behaviour: no reporting once sending has started."""

    async def _send_response(self, request, send, response):  # type: ignore[no-untyped-def]
        from hypercorn.typing import (
            HTTPResponseBodyEvent,
            HTTPResponseStartEvent,
        )
        from typing import cast

        from quart.utils import encode_headers
        from quart.wrappers.base import ClientDisconnectedError
        from werkzeug.wrappers import Response as WerkzeugResponse

        if self._disconnected:
            raise ClientDisconnectedError()
        await send(
            cast(
                HTTPResponseStartEvent,
                {
                    "type": "http.response.start",
                    "status": response.status_code,
                    "headers": encode_headers(response.headers),
                },
            )
        )
        if isinstance(response, WerkzeugResponse):
            for data in response.response:
                body = data.encode() if isinstance(data, str) else data
                await send(cast(HTTPResponseBodyEvent, {
                    "type": "http.response.body", "body": body, "more_body": True}))
        else:
            async with response.response as response_body:
                async for data in response_body:
                    body = data.encode() if isinstance(data, str) else data
                    await send(cast(HTTPResponseBodyEvent, {
                        "type": "http.response.body",
                        "body": body, "more_body": True}))
        await send(cast(HTTPResponseBodyEvent, {
            "type": "http.response.body", "body": b"", "more_body": False}))


@dataclass
class ProbeResult:
    sent: list[dict[str, Any]]
    boundary_exc: BaseException | None
    logs: list[logging.LogRecord]
    trace: Trace
    signals: list[str] = field(default_factory=list)

    @property
    def response_started(self) -> bool:
        return any(e["type"] == "http.response.start" for e in self.sent)

    @property
    def body_bytes(self) -> bytes:
        return b"".join(
            e.get("body", b"") for e in self.sent if e["type"] == "http.response.body"
        )

    @property
    def terminated_cleanly(self) -> bool:
        return any(
            e["type"] == "http.response.body" and not e.get("more_body", True)
            for e in self.sent
        )


async def drive(app: Quart, trace: Trace, log_handler: ListHandler) -> ProbeResult:
    scope = {
        "type": "http",
        "asgi": {"spec_version": "2.1"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/download",
        "raw_path": b"/download",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"localhost")],
        "client": ("127.0.0.1", 51234),
        "server": ("localhost", 80),
        "extensions": {},
        "state": {},
    }

    inbound: asyncio.Queue = asyncio.Queue()
    inbound.put_nowait({"type": "http.request", "body": b"", "more_body": False})

    async def receive() -> dict[str, Any]:
        message = await inbound.get()
        if message["type"] == "http.request" and not message.get("more_body", False):
            # Mirror a real socket: after the request body the peer is able to
            # send a disconnect so the TaskGroup message loop can terminate.
            inbound.put_nowait({"type": "http.disconnect"})
        return message

    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        kind = message["type"]
        if kind == "http.response.start":
            trace.add(
                "ASGI-SEND",
                f"response.start status={message['status']} "
                f"headers={[(k.decode(), v.decode()) for k, v in message['headers']]}",
            )
        elif kind == "http.response.body":
            trace.add(
                "ASGI-SEND",
                f"response.body bytes={message.get('body', b'')!r} "
                f"more_body={message.get('more_body', True)}",
            )
        sent.append(message)

    boundary_exc: BaseException | None = None
    fired_signals: list[str] = []

    def _on_serving_exception(sender: Any, *, exception: BaseException, **_: Any) -> None:
        fired_signals.append(f"got_serving_exception({type(exception).__name__})")

    got_serving_exception.connect(_on_serving_exception, app)
    try:
        await app(scope, receive, send)
        trace.add("ASGI-END", "connection callable returned normally")
    except BaseException as exc:  # noqa: BLE001 - we want everything, incl. groups
        boundary_exc = exc
        trace.add("ASGI-END", f"boundary raised {type(exc).__name__}: {exc}")
    finally:
        got_serving_exception.disconnect(_on_serving_exception, app)

    for name in fired_signals:
        trace.add("SIGNAL", name)
    return ProbeResult(sent, boundary_exc, list(log_handler.records), trace, fired_signals)


# --------------------------------------------------------------------------- #
# Application factory: each failure can be switched on independently
# --------------------------------------------------------------------------- #
def build_app(
    trace: Trace,
    *,
    permission_revoked: bool,
    generator_boom: bool,
    teardown_boom: bool,
    log_handler: ListHandler,
    legacy: bool = False,
) -> Quart:
    app = Quart(__name__)
    app.testing = False  # production behaviour: do NOT propagate from handle_exception
    # Quart installs a QueueHandler on the app logger; attach at the root as
    # well so records are captured in-process regardless of that handler chain.
    logging.getLogger().addHandler(log_handler)
    logging.getLogger().setLevel(logging.DEBUG)
    app.logger.addHandler(log_handler)
    app.logger.setLevel(logging.DEBUG)
    if legacy:
        app.asgi_http_class = LegacyASGIHTTPConnection

    @app.errorhandler(GeneratorBoomError)
    async def generator_boom_handler(error: GeneratorBoomError):  # type: ignore[no-untyped-def]
        trace.add("ERROR-HANDLER", f"invoked for {type(error).__name__}")
        return "generator error page", 500

    @app.errorhandler(PermissionRevokedError)
    async def permission_handler(error: PermissionRevokedError):  # type: ignore[no-untyped-def]
        trace.add("ERROR-HANDLER", f"invoked for {type(error).__name__}")
        return "authorization revoked", 403

    @app.teardown_request
    async def fail_in_teardown(exc: BaseException | None) -> None:
        trace.add(
            "TEARDOWN-ENTER",
            f"teardown_request sees exc={type(exc).__name__ if exc else None}",
        )
        if teardown_boom:
            trace.add("TEARDOWN-RAISE", f"{TeardownBoomError.__name__} (cleanup failure)")
            raise TeardownBoomError("cleanup of download resource failed")

    @app.get("/download")
    async def download():  # type: ignore[no-untyped-def]
        async def stream():  # type: ignore[no-untyped-def]
            trace.add("VIEW", "authorization checked: granted, starting stream")
            yield b"chunk-1"
            trace.add("GENERATOR", "chunk-1 yielded and flushed")
            if permission_revoked:
                trace.add(
                    "GENERATOR-RAISE",
                    f"{PermissionRevokedError.__name__}: token revoked mid-stream",
                )
                raise PermissionRevokedError("download token revoked")
            yield b"chunk-2"
            trace.add("GENERATOR", "chunk-2 yielded and flushed")
            if generator_boom:
                trace.add("GENERATOR-RAISE", f"{GeneratorBoomError.__name__}: body source died")
                raise GeneratorBoomError("body source exploded")
            yield b"chunk-3"

        trace.add("VIEW", "returning streaming Response (200 OK)")
        return app.response_class(stream(), status=200, mimetype="application/octet-stream")

    # Instrument context pop so teardown ordering is visible even when it wins.
    orig_pop = AppContext.pop

    async def traced_pop(  # type: ignore[no-untyped-def]
        self: AppContext, exc: BaseException | None = None
    ) -> None:
        trace.add(
            "CTX-POP",
            f"AppContext.pop(exc={type(exc).__name__ if exc else None})",
        )
        try:
            return await orig_pop(self, exc)
        except BaseException as exc2:  # noqa: BLE001
            trace.add("CTX-POP-ESCAPE", f"{type(exc2).__name__}: {exc2}")
            raise

    AppContext.pop = traced_pop  # type: ignore[method-assign]
    return app


def flatten(exc: BaseException | None, depth: int = 0) -> list[str]:
    if exc is None:
        return []
    lines = ["  " * depth + f"- {type(exc).__name__}: {exc}"]
    if isinstance(exc, BaseExceptionGroup):
        for sub in exc.exceptions:
            lines.extend(flatten(sub, depth + 1))
    cause = exc.__cause__
    if cause is not None and cause is not exc:
        lines.append("  " * depth + "  __cause__:")
        lines.extend(flatten(cause, depth + 2))
    return lines


SCENARIOS = [
    ("S0 baseline (no failures)", dict(
        permission_revoked=False, generator_boom=False, teardown_boom=False)),
    ("S1 permission revoked mid-stream + teardown failure", dict(
        permission_revoked=True, generator_boom=False, teardown_boom=True)),
    ("S2 generator failure mid-stream + teardown failure", dict(
        permission_revoked=False, generator_boom=True, teardown_boom=True)),
    ("S3 teardown failure only (pre-first-byte)", dict(
        permission_revoked=False, generator_boom=False, teardown_boom=True)),
    ("S4 permission revoked only (post-first-byte)", dict(
        permission_revoked=True, generator_boom=False, teardown_boom=False)),
    ("S5 generator failure only (post-first-byte)", dict(
        permission_revoked=False, generator_boom=True, teardown_boom=False)),
    ("S6 all three failures at once", dict(
        permission_revoked=True, generator_boom=True, teardown_boom=True)),
]


async def run_one(title: str, flags: dict[str, bool], legacy: bool) -> ProbeResult:
    trace = Trace()
    log_handler = ListHandler()
    app = build_app(trace, log_handler=log_handler, legacy=legacy, **flags)
    try:
        return await drive(app, trace, log_handler)
    finally:
        logging.getLogger().removeHandler(log_handler)


def boundary_name(exc: BaseException | None) -> str:
    if exc is None:
        return "none"
    if isinstance(exc, BaseExceptionGroup):
        leaves = [type(e).__name__ for e in exc.exceptions]
        return f"ExceptionGroup[{', '.join(leaves)}]"
    return type(exc).__name__


async def main() -> int:
    # Detailed causal timelines for the two boundary cases, in fixed mode.
    for title, flags in [
        ("DETAIL A: teardown failure (pre-first-byte)", SCENARIOS[3][1]),
        ("DETAIL B: permission revoked (post-first-byte)", SCENARIOS[4][1]),
    ]:
        result = await run_one(title, flags, legacy=False)
        print("=" * 88)
        print(title)
        print("-" * 88)
        print(result.trace.render())
        print("-" * 88)
        print(f"response.start sent      : {result.response_started}")
        print(f"clean terminal frame     : {result.terminated_cleanly}")
        print(f"bytes already on the wire: {result.body_bytes!r} ({len(result.body_bytes)} B)")
        print(f"serving signal fired     : {result.signals or '<none>'}")
        if result.logs:
            for record in result.logs:
                print(f"app log [{record.levelname}] {record.getMessage()}")
        else:
            print("app log                   : <none>")
        print("boundary exception tree  :")
        print("\n".join(flatten(result.boundary_exc)) if result.boundary_exc else "  none")

    print("=" * 88)
    print("SIDE-BY-SIDE: pre-fix (legacy) vs post-fix sending pipeline")
    print("-" * 88)
    header = f"{'scenario':48s} | {'mode':6s} | start | bytes | terminal | signals | boundary"
    print(header)
    print("-" * len(header))
    for title, flags in SCENARIOS:
        for mode, legacy in (("legacy", True), ("fixed", False)):
            result = await run_one(title, flags, legacy)
            print(
                f"{title[:48]:48s} | {mode:6s} | "
                f"{str(result.response_started):5s} | "
                f"{len(result.body_bytes):5d}B | "
                f"{str(result.terminated_cleanly):8s} | "
                f"{len(result.signals):7d} | {boundary_name(result.boundary_exc)}"
            )
        print("-" * len(header))
    return 0


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        raise SystemExit(asyncio.run(main()))
