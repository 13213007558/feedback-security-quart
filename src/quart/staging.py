from __future__ import annotations

import inspect
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import IO
from typing import Protocol
from typing import TYPE_CHECKING
from typing import runtime_checkable

from werkzeug.datastructures import Headers
from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import RequestEntityTooLarge

if TYPE_CHECKING:
    from .formparser import StreamFactory


class FieldAdmissionError(ValueError):
    """Raised when a part is refused admission to the staging area.

    The bytes of a refused part are consumed and discarded without ever
    being handed to a stream factory, therefore no temporary file is
    created for them. This is a :class:`ValueError` so the existing
    ``silent`` behaviour of :class:`~quart.formparser.FormDataParser`
    applies unchanged.
    """


class StagingQuotaExceeded(FieldAdmissionError, RequestEntityTooLarge):
    """Raised when a staging category would exceed its byte quota.

    The check happens before the offending bytes are written, so a quota
    breach never leaves partial data on disk. As a
    :class:`~werkzeug.exceptions.RequestEntityTooLarge` it maps to an
    HTTP ``413`` response when errors are not silenced, while the
    :class:`ValueError` base keeps it compatible with the parser's
    ``silent`` mode.
    """


@dataclass(frozen=True)
class StagingDecision:
    """How an admitted file part should be staged.

    Use :meth:`spool` for parts that may be written to a temporary file
    and :meth:`discard` for parts that must be consumed but never stored.
    """

    spool: bool
    category: str
    reason: str | None = None

    @staticmethod
    def spool_decision(category: str, reason: str | None = None) -> StagingDecision:
        return StagingDecision(True, category, reason)

    @staticmethod
    def discard(reason: str | None = None) -> StagingDecision:
        return StagingDecision(False, "", reason)


@dataclass(frozen=True)
class AdmissionContext:
    """Information available to a :class:`FieldAdmissionPolicy`.

    Only the multipart headers for the current part and the regular form
    fields already parsed before this part are available. A policy must
    not rely on fields that appear later in the body, as field ordering in
    a ``multipart/form-data`` request is not guaranteed.
    """

    name: str
    filename: str | None
    headers: Headers
    parsed_fields: MultiDict
    content_length: int | None


FieldAdmissionPolicyReturn = StagingDecision | Awaitable[StagingDecision]


@runtime_checkable
class FieldAdmissionPolicy(Protocol):
    """Decide the staging strategy for a single file part.

    Implementations may be synchronous or asynchronous; the parser
    transparently awaits coroutine results. The decision is requested
    after the part headers have been parsed but before any data is read
    or any temporary file is opened.
    """

    def __call__(self, context: AdmissionContext) -> FieldAdmissionPolicyReturn: ...


class AllowAllAdmissionPolicy:
    """Default policy: admit every file part under the ``"default"`` category."""

    def __call__(self, context: AdmissionContext) -> StagingDecision:
        return StagingDecision.spool_decision("default")


async def resolve_decision(
    policy: FieldAdmissionPolicy, context: AdmissionContext
) -> StagingDecision:
    """Invoke a policy, awaiting it when it returns a coroutine."""

    result = policy(context)
    if inspect.isawaitable(result):
        return await result
    return result


class SpoolRegistry:
    """Owns every stream opened while parsing one multipart request.

    The registry is the single place that knows which temporary streams
    exist and which quota category each one belongs to. Bytes are written
    through :meth:`write`, which enforces the per-category quota before
    touching the stream. On any failure :meth:`close_all` deterministically
    closes every still-open stream, while a successful parse calls
    :meth:`detach_all` to transfer ownership to the returned file storages.
    """

    def __init__(
        self,
        stream_factory: StreamFactory,
        quota: dict[str, int] | None = None,
        total_content_length: int | None = None,
    ) -> None:
        self._stream_factory = stream_factory
        self._quota = dict(quota) if quota is not None else {}
        self._total_content_length = total_content_length
        self._handles: dict[IO[bytes], str] = {}
        self._category_usage: dict[str, int] = {}

    @property
    def category_usage(self) -> dict[str, int]:
        return dict(self._category_usage)

    def _check_quota(self, category: str, projected: int) -> None:
        limit = self._quota.get(category)
        if limit is not None and projected > limit:
            raise StagingQuotaExceeded(
                f"Staging category {category!r} quota of {limit} bytes exceeded"
            )

    def open(
        self,
        category: str,
        content_type: str | None,
        filename: str | None,
        declared_length: int | None,
    ) -> IO[bytes]:
        if declared_length is not None and declared_length > 0:
            self._check_quota(
                category, self._category_usage.get(category, 0) + declared_length
            )

        stream = self._stream_factory(
            self._total_content_length,
            content_type,
            filename,
            declared_length,
        )
        self._handles[stream] = category
        self._category_usage.setdefault(category, 0)
        return stream

    def write(self, stream: IO[bytes], data: bytes) -> None:
        category = self._handles[stream]
        projected = self._category_usage[category] + len(data)
        self._check_quota(category, projected)
        stream.write(data)
        self._category_usage[category] = projected

    def detach_all(self) -> None:
        """Release ownership without closing the streams."""

        self._handles.clear()

    def close_all(self) -> None:
        """Close every stream still owned by the registry.

        Closing happens in reverse opening order so that partially created
        parts are torn down before the parts they depend on. A failure to
        close one stream never prevents the remaining streams from being
        closed.
        """

        while self._handles:
            stream, _category = self._handles.popitem()
            try:
                stream.close()
            except Exception:
                continue
