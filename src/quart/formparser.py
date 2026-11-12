from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any
from typing import IO
from typing import NoReturn
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl

from werkzeug.datastructures import Headers
from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.formparser import default_stream_factory
from werkzeug.http import parse_options_header
from werkzeug.sansio.multipart import Data
from werkzeug.sansio.multipart import Epilogue
from werkzeug.sansio.multipart import Field
from werkzeug.sansio.multipart import File
from werkzeug.sansio.multipart import MultipartDecoder
from werkzeug.sansio.multipart import NeedData

from .datastructures import FileStorage
from .staging import AdmissionContext
from .staging import AllowAllAdmissionPolicy
from .staging import FieldAdmissionPolicy
from .staging import resolve_decision
from .staging import SpoolRegistry
from .staging import StagingDecision

if TYPE_CHECKING:
    from .wrappers.request import Body

StreamFactory = Callable[
    [int | None, str | None, str | None, int | None],
    IO[bytes],
]

ParserFunc = Callable[
    ["FormDataParser", "Body", str, int | None, dict[str, str]],
    Awaitable[tuple[MultiDict, MultiDict]],
]


class FormDataParser:
    file_storage_class = FileStorage

    def __init__(
        self,
        *,
        cls: type[MultiDict] | None = MultiDict,
        max_content_length: int | None = None,
        max_form_memory_size: int | None = None,
        max_form_parts: int | None = None,
        silent: bool = True,
        stream_factory: StreamFactory = default_stream_factory,
        field_admission_policy: FieldAdmissionPolicy | None = None,
        spool_quota: dict[str, int] | None = None,
    ) -> None:
        self.cls = cls
        self.max_content_length = max_content_length
        self.max_form_memory_size = max_form_memory_size
        self.max_form_parts = max_form_parts
        self.silent = silent
        self.stream_factory = stream_factory
        self.field_admission_policy = (
            field_admission_policy
            if field_admission_policy is not None
            else AllowAllAdmissionPolicy()
        )
        self.spool_quota = spool_quota

    def get_parse_func(
        self, mimetype: str, options: dict[str, str]
    ) -> ParserFunc | None:
        return self.parse_functions.get(mimetype)

    async def parse(
        self,
        body: Body,
        mimetype: str,
        content_length: int | None,
        options: dict[str, str] | None = None,
    ) -> tuple[MultiDict, MultiDict]:
        if options is None:
            options = {}

        parse_func = self.get_parse_func(mimetype, options)

        if parse_func is not None:
            try:
                return await parse_func(self, body, mimetype, content_length, options)
            except ValueError:
                if not self.silent:
                    raise

        return self.cls(), self.cls()

    async def _parse_multipart(
        self,
        body: Body,
        mimetype: str,
        content_length: int | None,
        options: dict[str, str],
    ) -> tuple[MultiDict, MultiDict]:
        parser = MultiPartParser(
            cls=self.cls,
            file_storage_cls=self.file_storage_class,
            max_content_length=self.max_content_length,
            max_form_memory_size=self.max_form_memory_size,
            max_form_parts=self.max_form_parts,
            stream_factory=self.stream_factory,
            field_admission_policy=self.field_admission_policy,
            spool_quota=self.spool_quota,
        )
        boundary = options.get("boundary", "").encode("ascii")

        if not boundary:
            raise ValueError("Missing boundary")

        return await parser.parse(body, boundary, content_length)

    async def _parse_urlencoded(
        self,
        body: Body,
        mimetype: str,
        content_length: int | None,
        options: dict[str, str],
    ) -> tuple[MultiDict, MultiDict]:
        form = parse_qsl(
            (await body).decode(),
            keep_blank_values=True,
        )
        return self.cls(form), self.cls()

    parse_functions: dict[str, ParserFunc] = {
        "multipart/form-data": _parse_multipart,
        "application/x-www-form-urlencoded": _parse_urlencoded,
        "application/x-url-encoded": _parse_urlencoded,
    }


class MultiPartParser:
    def __init__(
        self,
        *,
        buffer_size: int = 64 * 1024,
        cls: type[MultiDict] = MultiDict,
        file_storage_cls: type[FileStorage] = FileStorage,
        max_content_length: int | None = None,
        max_form_memory_size: int | None = None,
        max_form_parts: int | None = None,
        stream_factory: StreamFactory = default_stream_factory,
        field_admission_policy: FieldAdmissionPolicy | None = None,
        spool_quota: dict[str, int] | None = None,
    ) -> None:
        self.buffer_size = buffer_size
        self.cls = cls
        self.file_storage_cls = file_storage_cls
        self.max_content_length = max_content_length
        self.max_form_memory_size = max_form_memory_size
        self.max_form_parts = max_form_parts
        self.stream_factory = stream_factory
        self.field_admission_policy = (
            field_admission_policy
            if field_admission_policy is not None
            else AllowAllAdmissionPolicy()
        )
        self.spool_quota = spool_quota

    def fail(self, message: str) -> NoReturn:
        raise ValueError(message)

    def get_part_charset(self, headers: Headers) -> str:
        content_type = headers.get("content-type")

        if content_type:
            parameters = parse_options_header(content_type)[1]
            ct_charset = parameters.get("charset", "").lower()

            # A safe list of encodings. Modern clients should only send ASCII or UTF-8.
            # This list will not be extended further.
            if ct_charset in {"ascii", "us-ascii", "utf-8", "iso-8859-1"}:
                return ct_charset

        return "utf-8"

    def _declared_content_length(self, event: File) -> int | None:
        try:
            return int(event.headers["content-length"])
        except (KeyError, ValueError):
            return None

    def start_file_streaming(
        self, event: File, registry: SpoolRegistry, decision: StagingDecision
    ) -> IO[bytes]:
        content_type = event.headers.get("content-type")
        return registry.open(
            decision.category,
            content_type,
            event.filename,
            self._declared_content_length(event),
        )

    async def parse(
        self, body: Body, boundary: bytes, content_length: int
    ) -> tuple[MultiDict, MultiDict]:
        container: IO[bytes] | list[bytes]
        _write: Callable[[bytes], Any]

        parser = MultipartDecoder(
            boundary, self.max_content_length, max_parts=self.max_form_parts
        )

        registry = SpoolRegistry(
            self.stream_factory,
            self.spool_quota,
            total_content_length=content_length,
        )
        fields: list[tuple[str, str]] = []
        files: list[tuple[str, FileStorage]] = []

        current_part: Field | File
        current_stream: IO[bytes] | None = None
        field_size: int | None = None
        try:
            async for data in body:
                parser.receive_data(data)
                event = parser.next_event()
                while not isinstance(event, (Epilogue, NeedData)):
                    if isinstance(event, Field):
                        current_part = event
                        current_stream = None
                        field_size = 0
                        container: IO[bytes] | list[bytes] = []
                        _write = container.append
                    elif isinstance(event, File):
                        current_part = event
                        field_size = None
                        current_stream = None
                        decision = await resolve_decision(
                            self.field_admission_policy,
                            AdmissionContext(
                                name=event.name,
                                filename=event.filename,
                                headers=event.headers,
                                parsed_fields=self.cls(fields),
                                content_length=self._declared_content_length(event),
                            ),
                        )
                        if decision.spool:
                            # The stream is only opened once the part has been
                            # admitted, so a refused part never reaches disk.
                            current_stream = self.start_file_streaming(
                                event, registry, decision
                            )
                    elif isinstance(event, Data):
                        if (
                            self.max_form_memory_size is not None
                            and field_size is not None
                        ):
                            field_size += len(event.data)
                            if field_size > self.max_form_memory_size:
                                raise RequestEntityTooLarge()

                        if current_stream is not None:
                            # Quota is enforced inside the registry before the
                            # bytes are written.
                            registry.write(current_stream, event.data)
                        elif isinstance(current_part, Field):
                            _write(event.data)
                        # A refused File part has no stream: its bytes are read
                        # to advance the parser and then dropped without storage.

                        if not event.more_data:
                            if isinstance(current_part, Field):
                                charset = self.get_part_charset(
                                    current_part.headers
                                )
                                value = b"".join(container).decode(
                                    charset, "replace"
                                )
                                fields.append((current_part.name, value))
                            elif current_stream is not None:
                                current_stream.seek(0)
                                files.append(
                                    (
                                        current_part.name,
                                        self.file_storage_cls(
                                            current_stream,
                                            current_part.filename,
                                            current_part.name,
                                            headers=current_part.headers,
                                        ),
                                    )
                                )

                    event = parser.next_event()
        except BaseException:
            # Any failure (admission refusal, quota, malformed body, client
            # disconnect) must not leave admitted data behind. Streams handed
            # to file storages are still owned by the registry until success.
            registry.close_all()
            raise

        # Success: the open streams now back the returned FileStorage objects.
        registry.detach_all()
        return self.cls(fields), self.cls(files)
