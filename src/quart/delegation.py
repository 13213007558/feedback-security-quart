from __future__ import annotations

from collections.abc import Iterable
from contextvars import ContextVar
from contextvars import Token
from types import TracebackType
from typing import Self


class CapabilityDeniedError(PermissionError):
    """Raised when a delegated scope lacks a required capability.

    The message names only the missing capabilities. It deliberately
    does not include the delegating subject, the active request, or any
    other parent scope data, so it is safe to return to clients.
    """

    def __init__(self, missing: Iterable[str]) -> None:
        self.missing: frozenset[str] = frozenset(missing)
        formatted = ", ".join(sorted(self.missing))
        super().__init__(f"Missing capabilities: {formatted}")


class DelegationContext:
    """An immutable record of an explicitly granted capability subset.

    Each context links to the scope it was derived from, forming a
    provenance chain. A context can only ever be created as a subset of
    its parent, so delegated code cannot widen its own authority.
    """

    __slots__ = ("capabilities", "label", "parent", "subject")

    def __init__(
        self,
        capabilities: Iterable[str],
        *,
        label: str | None = None,
        subject: str | None = None,
        parent: DelegationContext | None = None,
    ) -> None:
        capabilities = frozenset(capabilities)
        if parent is not None and not capabilities <= parent.capabilities:
            raise CapabilityDeniedError(capabilities - parent.capabilities)
        self.capabilities: frozenset[str] = capabilities
        self.label = label
        self.subject = subject
        self.parent = parent

    @property
    def depth(self) -> int:
        """The nesting level of this delegation, 0 for a root grant."""
        depth = 0
        node = self.parent
        while node is not None:
            depth += 1
            node = node.parent
        return depth

    def narrow(
        self,
        capabilities: Iterable[str],
        *,
        label: str | None = None,
        subject: str | None = None,
    ) -> Self:
        """Derive a further restricted context from this one."""
        return type(self)(capabilities, label=label, subject=subject, parent=self)

    def provenance(self) -> tuple[DelegationContext, ...]:
        """The chain of grants from the root down to this context."""
        chain: list[DelegationContext] = []
        node: DelegationContext | None = self
        while node is not None:
            chain.append(node)
            node = node.parent
        return tuple(reversed(chain))

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__} capabilities={sorted(self.capabilities)}"
            f" depth={self.depth}>"
        )


_cv_delegation: ContextVar[DelegationContext | None] = ContextVar(
    "quart.delegation", default=None
)


class _DelegationScope:
    def __init__(
        self,
        capabilities: Iterable[str],
        *,
        label: str | None = None,
        subject: str | None = None,
    ) -> None:
        self._granted = frozenset(capabilities)
        self._label = label
        self._subject = subject
        self._context: DelegationContext | None = None
        self._token: Token[DelegationContext | None] | None = None

    def __enter__(self) -> DelegationContext:
        parent = _cv_delegation.get()
        self._context = DelegationContext(
            self._granted,
            label=self._label,
            subject=self._subject,
            parent=parent,
        )
        self._token = _cv_delegation.set(self._context)
        return self._context

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _cv_delegation.reset(self._token)
            self._token = None


def delegate(
    capabilities: Iterable[str],
    *,
    label: str | None = None,
    subject: str | None = None,
) -> _DelegationScope:
    """Enter a delegated scope with a narrowed set of capabilities.

    The granted capabilities must be a subset of the currently active
    delegation (if any), otherwise :class:`CapabilityDeniedError` is
    raised naming the capabilities that cannot be granted. On exit the
    previous delegation is restored, including when the block raises.

    .. code-block:: python

        with delegate({"read:reports"}, label="report-job"):
            ...

    """
    return _DelegationScope(capabilities, label=label, subject=subject)


def current_delegation() -> DelegationContext | None:
    """The active delegation, or None if the original (full) identity
    authorization applies, such as within a plain request."""
    return _cv_delegation.get()


def is_delegated() -> bool:
    """True if execution is within a delegated scope rather than under
    the original identity authorization."""
    return _cv_delegation.get() is not None


def current_capabilities() -> frozenset[str] | None:
    """The effective capabilities, or None when unrestricted (no
    delegation boundary is active)."""
    delegation = _cv_delegation.get()
    if delegation is None:
        return None
    return delegation.capabilities


def has_capability(capability: str) -> bool:
    """True if the capability is available. Always true outside a
    delegated scope, where the original authorization applies."""
    delegation = _cv_delegation.get()
    if delegation is None:
        return True
    return capability in delegation.capabilities


def require_capability(*capabilities: str) -> None:
    """Raise :class:`CapabilityDeniedError` if any of the capabilities
    are not available in the current delegated scope. Outside a
    delegated scope the original authorization applies and this is a
    no-op."""
    delegation = _cv_delegation.get()
    if delegation is None:
        return
    missing = set(capabilities) - delegation.capabilities
    if missing:
        raise CapabilityDeniedError(missing)
