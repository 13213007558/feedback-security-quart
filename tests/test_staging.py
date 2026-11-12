from __future__ import annotations

from io import BytesIO
from typing import IO

import pytest
from werkzeug.datastructures import Headers
from werkzeug.exceptions import RequestEntityTooLarge

from quart.formparser import FormDataParser
from quart.formparser import MultiPartParser
from quart.staging import AdmissionContext
from quart.staging import FieldAdmissionError
from quart.staging import StagingDecision
from quart.staging import StagingQuotaExceeded
from quart.wrappers.base import ClientDisconnectedError
from quart.wrappers.request import Body

BOUND = b"bnd"


class TrackedStream(BytesIO):
    """A BytesIO that records whether it was created and closed."""

    def __init__(self, factory: TrackedStreamFactory, tag: str) -> None:
        super().__init__()
        self._factory = factory
        self.tag = tag
        self.closed_flag = False
        factory.created.append(self)

    def close(self) -> None:  # type: ignore[override]
        self.closed_flag = True
        self._factory.closed.append(self)
        super().close()


class TrackedStreamFactory:
    """Mimics the werkzeug stream factory while staying in memory.

    Every stream it hands out is recorded so tests can assert a refused
    part never caused a stream to be opened and that failure closes all
    admitted streams.
    """

    def __init__(self) -> None:
        self.created: list[TrackedStream] = []
        self.closed: list[TrackedStream] = []
        self.calls_by_filename: dict[str, int] = {}

    def __call__(
        self,
        total_content_length: int | None,
        content_type: str | None,
        filename: str | None,
        content_length: int | None = None,
    ) -> IO[bytes]:
        key = filename or "<none>"
        self.calls_by_filename[key] = self.calls_by_filename.get(key, 0) + 1
        return TrackedStream(self, key)


def make_body(chunks: list[bytes]) -> Body:
    body = Body(None, None)
    body.set_result(b"".join(chunks))
    return body


def part(field: str, content: bytes, filename: str | None = None) -> bytes:
    if filename is None:
        head = (
            b'--' + BOUND + b'\r\nContent-Disposition: form-data; name="'
            + field.encode() + b'"\r\n\r\n'
        )
        return head + content + b"\r\n"
    head = (
        b'--' + BOUND + b'\r\nContent-Disposition: form-data; name="'
        + field.encode() + b'"; filename="' + filename.encode() + b'"\r\n'
        b"Content-Type: application/octet-stream\r\n\r\n"
    )
    return head + content + b"\r\n"


def finish() -> bytes:
    return b"--" + BOUND + b"--\r\n"


def chunked(data: bytes, size: int = 3) -> list[bytes]:
    return [data[i : i + size] for i in range(0, len(data), size)]


class NamePolicy:
    """Admit based on the field name, assigning a category per field."""

    def __init__(self, allowed: set[str], categories: dict[str, str] | None = None):
        self.allowed = allowed
        self.categories = categories or {}
        self.seen: list[str] = []

    def __call__(self, context: AdmissionContext) -> StagingDecision:
        self.seen.append(context.name)
        if context.name not in self.allowed:
            return StagingDecision.discard("not allowed")
        category = self.categories.get(context.name, "default")
        return StagingDecision.spool_decision(category)


async def run(data: bytes, policy=None, quota=None, buffer_size=3):
    factory = TrackedStreamFactory()
    parser = MultiPartParser(
        buffer_size=buffer_size,
        stream_factory=factory,
        field_admission_policy=policy,
        spool_quota=quota,
    )
    form, files = await parser.parse(make_body(chunked(data)), BOUND, 0)
    return form, files, factory


@pytest.mark.parametrize("order", [("doc", "id_doc"), ("id_doc", "doc")])
async def test_field_order_independent_admission(order: tuple[str, str]) -> None:
    """The same verdict is reached regardless of part ordering."""

    pieces = {
        "doc": part("doc", b"ordinary", "ordinary.txt"),
        "id_doc": part("id_doc", b"passport", "passport.png"),
    }
    data = b"".join(pieces[name] for name in order) + finish()
    # Only the ordinary attachment is admitted.
    policy = NamePolicy({"doc"}, {"doc": "general"})
    form, files, factory = await run(data, policy)

    assert list(files.keys()) == ["doc"]
    assert files["doc"].stream.read() == b"ordinary"
    # The sensitive part was never handed to the stream factory.
    assert "passport.png" not in factory.calls_by_filename


async def test_duplicate_fields_each_evaluated_and_tracked() -> None:
    """Repeated file parts are admitted independently and all retained."""

    data = part("doc", b"one", "a.bin") + part("doc", b"two", "b.bin") + finish()
    policy = NamePolicy({"doc"}, {"doc": "general"})
    form, files, factory = await run(data, policy)

    assert files.getlist("doc") and len(files.getlist("doc")) == 2
    assert sorted(factory.calls_by_filename.values()) == [1, 1]
    assert policy.seen == ["doc", "doc"]


async def test_refused_part_never_opens_stream() -> None:
    """A denied identity material must not create any temporary file."""

    data = part("doc", b"keep", "ok.bin") + part(
        "id_doc", b"secret", "id.pdf"
    ) + finish()
    policy = NamePolicy({"doc"}, {"doc": "general"})
    form, files, factory = await run(data, policy)

    assert "id_doc" not in files
    assert set(factory.calls_by_filename) == {"ok.bin"}
    assert all(stream.tag == "ok.bin" for stream in factory.created)


async def test_per_category_quota_enforced_during_write() -> None:
    """Quota is checked per category and trips before the overflow write."""

    data = part("id_doc", b"x" * 10, "big.bin") + finish()
    policy = NamePolicy({"id_doc"}, {"id_doc": "sensitive"})
    with pytest.raises(StagingQuotaExceeded):
        await run(data, policy, quota={"sensitive": 5})


async def test_quota_isolates_categories() -> None:
    """A cap on one category does not constrain another."""

    data = (
        part("doc", b"y" * 12, "doc.bin")
        + part("id_doc", b"z" * 4, "id.bin")
        + finish()
    )
    policy = NamePolicy(
        {"doc", "id_doc"}, {"doc": "general", "id_doc": "sensitive"}
    )
    form, files, factory = await run(
        data, policy, quota={"sensitive": 5}
    )
    assert files["doc"].stream.read() == b"y" * 12
    assert files["id_doc"].stream.read() == b"z" * 4


async def test_declared_length_rejected_before_stream_opens() -> None:
    """A declared over-quota size trips at admission, before open()."""

    raw = part("id_doc", b"abc", "id.bin")
    # Inject an explicit Content-Length header on the file part.
    raw = raw.replace(
        b"Content-Type: application/octet-stream\r\n",
        b"Content-Type: application/octet-stream\r\nContent-Length: 100\r\n",
        1,
    )
    data = raw + finish()
    policy = NamePolicy({"id_doc"}, {"id_doc": "sensitive"})
    with pytest.raises(StagingQuotaExceeded):
        await run(data, policy, quota={"sensitive": 50})
    # No stream was created because the quota check happens in open().
    factory_checked = TrackedStreamFactory()
    parser = MultiPartParser(
        stream_factory=factory_checked,
        field_admission_policy=policy,
        spool_quota={"sensitive": 50},
    )
    with pytest.raises(StagingQuotaExceeded):
        await parser.parse(make_body(chunked(data)), BOUND, 0)
    assert factory_checked.created == []


async def test_failure_closes_all_admitted_streams() -> None:
    """Already spooled parts are cleaned up when a later part fails."""

    data = (
        part("doc", b"first", "a.bin")
        + part("id_doc", b"x" * 10, "big.bin")
        + finish()
    )
    policy = NamePolicy(
        {"doc", "id_doc"}, {"doc": "general", "id_doc": "sensitive"}
    )
    with pytest.raises(StagingQuotaExceeded):
        await run(data, policy, quota={"sensitive": 5})
    # Re-run with an inspectable factory to assert deterministic cleanup.
    factory = TrackedStreamFactory()
    parser = MultiPartParser(
        stream_factory=factory,
        field_admission_policy=policy,
        spool_quota={"sensitive": 5},
    )
    with pytest.raises(StagingQuotaExceeded):
        await parser.parse(make_body(chunked(data)), BOUND, 0)
    assert len(factory.created) >= 1
    assert all(stream.closed_flag for stream in factory.created)


async def test_malformed_body_cleans_up() -> None:
    """A parse failure (part missing Content-Disposition) closes open streams."""

    good = part("doc", b"data", "a.bin")
    data = (
        good
        + b"--" + BOUND + b"\r\nX-Bogus: yes\r\n\r\n"
        + b"junk bytes without a disposition\r\n"
        + b"--" + BOUND + b"--\r\n"
    )
    factory = TrackedStreamFactory()
    parser = MultiPartParser(stream_factory=factory)
    with pytest.raises(ValueError):
        await parser.parse(make_body(chunked(data)), BOUND, 0)
    assert all(stream.closed_flag for stream in factory.created)


async def test_client_disconnect_cleans_up() -> None:
    """Cancellation mid-stream closes everything already admitted."""

    data = part("doc", b"partial", "a.bin")
    body = Body(None, None)
    # Deliver the first chunk through the queue, then disconnect.
    chunks = chunked(data)
    for piece in chunks[:3]:
        body._queue.put_nowait(piece)
    body.disconnect()

    factory = TrackedStreamFactory()
    parser = MultiPartParser(stream_factory=factory)
    with pytest.raises(ClientDisconnectedError):
        await parser.parse(body, BOUND, 0)
    assert all(stream.closed_flag for stream in factory.created)


async def test_async_policy_is_awaited() -> None:
    """A coroutine-returning policy is transparently awaited."""

    class AsyncPolicy:
        async def __call__(self, context: AdmissionContext) -> StagingDecision:
            return StagingDecision.spool_decision("general")

    data = part("doc", b"abc", "a.bin") + finish()
    form, files, factory = await run(data, AsyncPolicy())
    assert files["doc"].stream.read() == b"abc"


async def test_policy_may_refuse_with_exception() -> None:
    """A policy raising FieldAdmissionError aborts and leaves no files."""

    class StrictPolicy:
        def __call__(self, context: AdmissionContext) -> StagingDecision:
            raise FieldAdmissionError("forbidden field")

    data = part("id_doc", b"abc", "id.bin") + finish()
    factory = TrackedStreamFactory()
    parser = MultiPartParser(
        stream_factory=factory, field_admission_policy=StrictPolicy()
    )
    with pytest.raises(FieldAdmissionError):
        await parser.parse(make_body(chunked(data)), BOUND, 0)
    assert factory.created == []


async def test_successful_parse_detaches_streams() -> None:
    """On success the backing streams remain open for the FileStorage."""

    data = part("doc", b"keep-me", "a.bin") + finish()
    form, files, factory = await run(data, NamePolicy({"doc"}))
    assert len(factory.created) == 1
    assert not factory.created[0].closed_flag
    assert files["doc"].stream is factory.created[0]


async def test_admission_context_exposes_parsed_fields() -> None:
    """Earlier regular fields are visible to the policy for decisions."""

    captured: list[AdmissionContext] = []

    class CapturingPolicy:
        def __call__(self, context: AdmissionContext) -> StagingDecision:
            captured.append(context)
            return StagingDecision.spool_decision("general")

    data = (
        part("declaration", b"agreed")
        + part("doc", b"abc", "a.bin")
        + finish()
    )
    await run(data, CapturingPolicy())
    ctx = captured[0]
    assert ctx.name == "doc"
    assert ctx.filename == "a.bin"
    assert ctx.parsed_fields["declaration"] == "agreed"
    assert isinstance(ctx.headers, Headers)


async def test_quota_error_is_http_413_and_value_error() -> None:
    """Quota breaches map to 413 while remaining silent-compatible."""

    assert issubclass(StagingQuotaExceeded, RequestEntityTooLarge)
    assert issubclass(StagingQuotaExceeded, ValueError)
    assert StagingQuotaExceeded("x").code == 413


async def test_silent_parser_returns_empty_on_refusal() -> None:
    """With the default silent parser a refusal yields empty results."""

    class StrictPolicy:
        def __call__(self, context: AdmissionContext) -> StagingDecision:
            raise FieldAdmissionError("denied")

    data = part("id_doc", b"abc", "id.bin") + finish()
    factory = TrackedStreamFactory()
    parser = FormDataParser(
        stream_factory=factory,
        field_admission_policy=StrictPolicy(),
        silent=True,
    )
    form, files = await parser.parse(
        make_body(chunked(data)),
        "multipart/form-data",
        0,
        {"boundary": BOUND.decode()},
    )
    assert form == {}
    assert files == {}
    assert factory.created == []


async def test_strict_parser_reraises_refusal() -> None:
    """With silent=False the admission error propagates to the caller."""

    class StrictPolicy:
        def __call__(self, context: AdmissionContext) -> StagingDecision:
            raise FieldAdmissionError("denied")

    data = part("id_doc", b"abc", "id.bin") + finish()
    parser = FormDataParser(
        field_admission_policy=StrictPolicy(),
        silent=False,
    )
    with pytest.raises(FieldAdmissionError):
        await parser.parse(
            make_body(chunked(data)),
            "multipart/form-data",
            0,
            {"boundary": BOUND.decode()},
        )
