"""Pluggable delivery auditing for sensitive-file downloads in Quart.

Public API:

* :class:`AuditState` / :class:`AuditRecord` -- lifecycle model.
* :class:`AuditStore` -- storage interface an application implements.
* :class:`InMemoryAuditStore` -- reference store with fault injection.
* :class:`AuditStoreError` / :class:`AuditTransitionError` -- failures.
* :func:`open_delivery` -- write the ``approved`` record.
* :func:`audited_response` -- wrap a body in an audited response.
* :func:`approved_delivery` -- one-call helper for files on disk.

Auditing is opt-in: only routes that call these functions produce
audit records; every other route is untouched.
"""

from __future__ import annotations

from .flow import approved_delivery
from .flow import audited_response
from .flow import begin_delivery
from .flow import open_delivery
from .models import AuditRecord
from .models import AuditState
from .models import transition_allowed
from .store import AuditStore
from .store import AuditStoreError
from .store import AuditTransitionError
from .store import InMemoryAuditStore

__all__ = [
    "AuditRecord",
    "AuditState",
    "AuditStore",
    "AuditStoreError",
    "AuditTransitionError",
    "InMemoryAuditStore",
    "approved_delivery",
    "audited_response",
    "begin_delivery",
    "open_delivery",
    "transition_allowed",
]
