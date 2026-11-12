"""Storage interface for delivery audit records.

The application owns the concrete store (database table, event log,
external audit service).  :class:`InMemoryAuditStore` is provided for
tests, local development and as a reference implementation; it also
supports deterministic fault injection so the delivery flow can be
exercised against a store that fails at any stage.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime

from .models import AuditRecord
from .models import AuditState
from .models import transition_allowed


class AuditStoreError(RuntimeError):
    """Raised when an audit write cannot be persisted."""


class AuditTransitionError(AuditStoreError):
    """Raised when a state transition is not allowed by the lifecycle."""


class AuditStore(ABC):
    """Persistence boundary for audit records.

    Every method is part of the security boundary: callers rely on a
    returned write being durable.  Implementations must raise
    :class:`AuditStoreError` (or a subclass) rather than swallowing
    failures, and must guarantee the compare-and-set semantics of
    :meth:`transition` even under concurrent calls.
    """

    @abstractmethod
    async def create(self, record: AuditRecord) -> AuditRecord:
        """Persist a freshly approved record and return it stored."""

    @abstractmethod
    async def get(self, audit_id: str) -> AuditRecord:
        """Return the stored record, or raise ``KeyError`` if unknown."""

    @abstractmethod
    async def progress(self, audit_id: str, bytes_delivered: int) -> None:
        """Persist the running delivered-byte counter."""

    @abstractmethod
    async def transition(
        self,
        audit_id: str,
        target: AuditState,
        *,
        expected: AuditState | None = None,
        error: str | None = None,
    ) -> AuditState:
        """Compare-and-set the record state.

        Args:
            audit_id: Record to update.
            target: Desired new state.
            expected: State the caller believes the record is in.  When
                given, the write only applies if the stored state still
                matches, making concurrent/duplicate cleanup safe.
            error: Optional error description to persist alongside the
                transition.

        Returns:
            The stored state after the call.  A no-op transition onto an
            already-terminal state returns that state instead of
            raising, so repeated cleanup never creates a second record
            or flips a completed delivery.
        """

    @abstractmethod
    def all(self) -> Iterator[AuditRecord]:
        """Iterate snapshot copies of every stored record."""


class InMemoryAuditStore(AuditStore):
    """Reference store with snapshot isolation and fault injection.

    All methods are coroutines so an asynchronous implementation (a
    database-backed store, for example) can be swapped in without
    changing call sites.  Reads return detached snapshots, so callers
    cannot mutate stored records by accident.

    Fault injection is configured with :meth:`fail_next` /
    :meth:`fail_always`.  A failure key is either an operation name
    (``create``, ``progress``, ``transition``) or an operation/state
    pair such as ``transition:started`` / ``transition:interrupted``,
    which only fails transitions into that state.
    """

    def __init__(self) -> None:
        self._records: dict[str, AuditRecord] = {}
        self._fail_once: set[str] = set()
        self._fail_always: set[str] = set()

    def fail_next(self, *keys: str) -> None:
        """Make the next matching operation(s) raise once, then recover."""

        self._fail_once.update(keys)

    def fail_always(self, *keys: str) -> None:
        """Make every matching operation raise until :meth:`recover`."""

        self._fail_always.update(keys)

    def recover(self, *keys: str) -> None:
        """Clear injected failures for ``keys`` (all of them if empty)."""

        if not keys:
            self._fail_once.clear()
            self._fail_always.clear()
            return
        for key in keys:
            self._fail_once.discard(key)
            self._fail_always.discard(key)

    def _maybe_fail(self, operation: str, state: AuditState | None) -> None:
        specific = f"{operation}:{state.value}" if state is not None else operation
        for key in (specific, operation):
            if key in self._fail_always:
                raise AuditStoreError(f"injected failure on {key}")
            if key in self._fail_once:
                self._fail_once.discard(key)
                raise AuditStoreError(f"injected failure on {key}")

    async def create(self, record: AuditRecord) -> AuditRecord:
        self._maybe_fail("create", record.state)
        if record.audit_id in self._records:
            raise AuditStoreError(f"duplicate audit id {record.audit_id}")
        stored = record.snapshot()
        self._records[stored.audit_id] = stored
        return stored.snapshot()

    async def get(self, audit_id: str) -> AuditRecord:
        try:
            return self._records[audit_id].snapshot()
        except KeyError:
            raise KeyError(f"unknown audit id {audit_id}") from None

    async def progress(self, audit_id: str, bytes_delivered: int) -> None:
        self._maybe_fail("progress", None)
        record = self._require(audit_id)
        record.bytes_delivered = bytes_delivered
        record.updated_at = datetime.now(UTC)

    async def transition(
        self,
        audit_id: str,
        target: AuditState,
        *,
        expected: AuditState | None = None,
        error: str | None = None,
    ) -> AuditState:
        record = self._require(audit_id)
        current = record.state
        if expected is not None and current != expected:
            # Lost a compare-and-set race (e.g. cleanup after completion).
            # Report the authoritative state without mutating anything.
            return current
        if current == target:
            return current
        if target.is_terminal and current.is_terminal:
            # Never overwrite one terminal state with another.
            return current
        if not transition_allowed(current, target):
            raise AuditTransitionError(
                f"cannot transition audit {audit_id} from {current.value} "
                f"to {target.value}"
            )
        self._maybe_fail("transition", target)
        record.state = target
        if error is not None:
            record.error = error
        record.updated_at = datetime.now(UTC)
        return record.state

    def all(self) -> Iterator[AuditRecord]:
        for record in self._records.values():
            yield record.snapshot()

    def _require(self, audit_id: str) -> AuditRecord:
        try:
            return self._records[audit_id]
        except KeyError:
            raise KeyError(f"unknown audit id {audit_id}") from None


def describe_error(error: BaseException) -> str:
    """Stable string form used when recording a failure reason."""

    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__
