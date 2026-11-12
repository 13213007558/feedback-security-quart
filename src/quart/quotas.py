"""Per-account authorization quotas for background tasks.

A :class:`QuotaService` holds one :class:`AccountQuota` per account.
Each account quota limits how many tasks may hold the account's
authorization (for example a signing service) at the same time. Tasks
that cannot be served immediately wait in a FIFO queue and only take
the permission once they actually start running.

The service can be closed, for example during application shutdown.
Once closed no further tasks are admitted and every waiting task is
revoked with :class:`ServiceClosedError`.

All public methods are safe to use from concurrent tasks. The in-use
counters can never leak: every granted lease is released exactly once,
including on task cancellation and on every error path.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .app import Quart

__all__ = (
    "AccountQuota",
    "QuotaLease",
    "QuotaService",
    "ServiceClosedError",
)


class ServiceClosedError(RuntimeError):
    """Raised when a quota is requested after the service was closed."""


class QuotaLease:
    """A single grant of an account's authorization resource.

    A lease is created by :meth:`AccountQuota.acquire` and must be
    released exactly once via :meth:`release`, or by using the lease as
    an asynchronous context manager. Releasing more than once is a no-op
    so that ``finally`` blocks cannot corrupt the in-use counter.
    """

    __slots__ = ("_released", "_release", "account_id")

    def __init__(
        self,
        account_id: str,
        release: Callable[[], Awaitable[None]],
    ) -> None:
        self.account_id = account_id
        self._release = release
        self._released = False

    @property
    def released(self) -> bool:
        return self._released

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._release()

    async def __aenter__(self) -> QuotaLease:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.release()


class AccountQuota:
    """A bounded, FIFO authorization quota for a single account."""

    def __init__(self, account_id: str, limit: int) -> None:
        if limit < 1:
            raise ValueError("Quota limit must be at least 1")
        self.account_id = account_id
        self._limit = limit
        self._in_use = 0
        self._closed = False
        self._lock = asyncio.Lock()
        # Waiters are woken in order so grants are handed out FIFO.
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def in_use(self) -> int:
        return self._in_use

    @property
    def waiting(self) -> int:
        return len(self._waiters)

    @property
    def closed(self) -> bool:
        return self._closed

    async def acquire(self) -> QuotaLease:
        """Wait until the account's resource is available.

        The permission is only taken when this coroutine returns, i.e.
        when the task is actually about to run. Raises
        :class:`ServiceClosedError` if the quota is closed while
        waiting, or if it had already been closed. If the awaiting
        task is cancelled the waiter is removed without taking a slot.
        """
        loop = asyncio.get_running_loop()
        async with self._lock:
            if self._closed:
                raise ServiceClosedError(
                    f"Quota service is closed for account {self.account_id!r}"
                )
            if self._in_use < self._limit and not self._waiters:
                self._in_use += 1
                return self._make_lease()
            waiter: asyncio.Future[None] = loop.create_future()
            self._waiters.append(waiter)

        try:
            await waiter
        except BaseException:
            # The waiter may have been granted a slot immediately before
            # it was cancelled. Give that slot back; otherwise simply
            # leave the queue. Revoked (closed) waiters never hold a slot.
            async with self._lock:
                granted = (
                    waiter.done()
                    and not waiter.cancelled()
                    and waiter.exception() is None
                )
                self._discard_waiter_locked(waiter)
                if granted:
                    self._in_use -= 1
                self._pump_locked()
            raise

        return self._make_lease()

    def close(self) -> None:
        """Close this quota and revoke every waiting task."""
        self._closed = True
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_exception(
                    ServiceClosedError(
                        f"Quota service is closed for account {self.account_id!r}"
                    )
                )

    async def _release(self) -> None:
        async with self._lock:
            self._in_use -= 1
            self._pump_locked()

    def _make_lease(self) -> QuotaLease:
        return QuotaLease(self.account_id, self._release)

    def _discard_waiter_locked(self, waiter: asyncio.Future[None]) -> None:
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass

    def _pump_locked(self) -> None:
        """Hand free slots to waiters, or revoke them once closed."""
        while self._waiters:
            waiter = self._waiters[0]
            if waiter.cancelled():
                self._waiters.popleft()
                continue
            if self._closed:
                self._waiters.popleft()
                if not waiter.done():
                    waiter.set_exception(
                        ServiceClosedError(
                            f"Quota service is closed for account "
                            f"{self.account_id!r}"
                        )
                    )
                continue
            if self._in_use >= self._limit:
                return
            self._waiters.popleft()
            if waiter.done():
                continue
            self._in_use += 1
            waiter.set_result(None)


class QuotaService:
    """Manages :class:`AccountQuota` instances keyed by account id."""

    def __init__(
        self,
        limits: dict[str, int] | None = None,
        *,
        app: Quart | None = None,
    ) -> None:
        self._quotas: dict[str, AccountQuota] = {}
        self._closed = False
        if limits is not None:
            for account_id, limit in limits.items():
                self._quotas[account_id] = AccountQuota(account_id, limit)
        if app is not None:
            app.quota_service = self

    @property
    def closed(self) -> bool:
        return self._closed

    def add_account(self, account_id: str, limit: int) -> AccountQuota:
        """Register an account quota. Fails once the service is closed."""
        if self._closed:
            raise ServiceClosedError("Quota service is closed")
        quota = AccountQuota(account_id, limit)
        self._quotas[account_id] = quota
        return quota

    def account(self, account_id: str) -> AccountQuota:
        try:
            return self._quotas[account_id]
        except KeyError:
            raise KeyError(
                f"No quota configured for account {account_id!r}"
            ) from None

    async def acquire(self, account_id: str) -> QuotaLease:
        return await self.account(account_id).acquire()

    @asynccontextmanager
    async def lease(self, account_id: str) -> AsyncIterator[QuotaLease]:
        """Acquire and automatically release a lease for an account."""
        quota_lease = await self.acquire(account_id)
        try:
            yield quota_lease
        finally:
            await quota_lease.release()

    def close(self) -> None:
        """Close every account quota and revoke all waiting tasks."""
        self._closed = True
        for quota in self._quotas.values():
            quota.close()
