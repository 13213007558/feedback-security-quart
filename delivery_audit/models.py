"""State model for sensitive-file delivery audits.

A single :class:`AuditRecord` follows a linear lifecycle::

    APPROVED -> STARTED -> COMPLETED
                       \\-> INTERRUPTED

``APPROVED`` is written before any byte of the file is handed to the
network stack.  ``STARTED`` is written in the view, before Quart emits
the response headers, so a failed write can still abort the delivery
with zero bytes sent.  From ``STARTED`` a delivery either reaches
``COMPLETED`` or ends in ``INTERRUPTED``; both states are terminal and
compare-and-set transitions make repeated cleanup (for example a
generator close racing a normal completion) idempotent.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4


class AuditState(str, Enum):
    """Lifecycle states a delivery audit record can be in."""

    APPROVED = "approved"
    STARTED = "started"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"

    @property
    def is_terminal(self) -> bool:
        return self in (AuditState.COMPLETED, AuditState.INTERRUPTED)


_ALLOWED_TRANSITIONS: dict[AuditState, frozenset[AuditState]] = {
    AuditState.APPROVED: frozenset(
        {AuditState.STARTED, AuditState.INTERRUPTED}
    ),
    AuditState.STARTED: frozenset(
        {AuditState.COMPLETED, AuditState.INTERRUPTED}
    ),
    AuditState.COMPLETED: frozenset(),
    AuditState.INTERRUPTED: frozenset(),
}


def transition_allowed(current: AuditState, target: AuditState) -> bool:
    """Return ``True`` if moving from ``current`` to ``target`` is valid."""

    return target == current or target in _ALLOWED_TRANSITIONS[current]


@dataclass
class AuditRecord:
    """An auditable sensitive-file delivery.

    Attributes:
        audit_id: Stable identifier shared by every state change of one
            delivery.  Returned to the client in ``X-Audit-Id`` so an
            operator can correlate the download with the record.
        file_ref: Application-defined reference to the delivered file
            (path, object key, document id, ...).
        requester: Application-defined identity of the requester.
        state: Current lifecycle state.
        bytes_total: Total size in bytes when known, otherwise ``None``.
        bytes_delivered: Number of bytes handed to the network so far.
        error: Last error reported against the delivery, if any.
        meta: Free-form application metadata captured at approval.
        created_at / updated_at: Timestamps in UTC.
    """

    file_ref: str
    requester: str | None = None
    audit_id: str = field(default_factory=lambda: uuid4().hex)
    state: AuditState = AuditState.APPROVED
    bytes_total: int | None = None
    bytes_delivered: int = 0
    error: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def snapshot(self) -> AuditRecord:
        """Return a detached copy that will not mutate with the record."""

        return AuditRecord(
            audit_id=self.audit_id,
            file_ref=self.file_ref,
            requester=self.requester,
            state=self.state,
            bytes_total=self.bytes_total,
            bytes_delivered=self.bytes_delivered,
            error=self.error,
            meta=dict(self.meta),
            created_at=self.created_at,
            updated_at=self.updated_at,
        )
