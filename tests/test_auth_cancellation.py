"""Cancellation contract tests for an asynchronous Quart authentication extension.

The extension follows three phases: it establishes a short-lived pending
authorization, waits for an external decision, and only then writes the
identity into the session.  Every phase has an event-driven local stand-in
so a request can be cancelled at a known point without sleeping or hoping
for a particular interleaving.

The contract for each cancellation is:

* the client does not receive an identity,
* the pending authorization is revoked exactly once, and
* the extension cleanup and Quart request teardown each run exactly once.

A variant that intentionally skips revocation is exercised at every phase
to prove that the contract assertions detect the leak.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from quart import Quart
from quart import request
from quart import session
from quart.sessions import SessionMixin
from quart.typing import ResponseReturnValue

PHASE_ESTABLISHING = "establishing"
PHASE_WAITING = "waiting"
PHASE_WRITING = "writing"
PHASES = (PHASE_ESTABLISHING, PHASE_WAITING, PHASE_WRITING)


class AuthorizationServiceError(Exception):
    """Raised by the local authorization stand-in for a failed decision."""


class PendingGrant:
    """A short-lived authorization grant held by the local stand-in."""

    def __init__(self, store: LocalGrantStore, token: str, subject: str) -> None:
        self._store = store
        self.token = token
        self.subject = subject
        self.activated = False

    async def activate(self) -> None:
        self._store.activation_entered.set()
        await self._store.activation_release.wait()
        self.activated = True

    async def revoke(self) -> None:
        await self._store.revoke(self)


class LocalGrantStore:
    """Records grant establishment and revocation without any real authority."""

    def __init__(self) -> None:
        self.started: list[PendingGrant] = []
        self.revoke_attempts = 0
        self.revoked_tokens: list[str] = []
        self.activation_entered = asyncio.Event()
        self.activation_release = asyncio.Event()

    def start(self, subject: str) -> PendingGrant:
        grant = PendingGrant(self, f"grant-{len(self.started) + 1}", subject)
        self.started.append(grant)
        return grant

    async def revoke(self, grant: PendingGrant) -> None:
        self.revoke_attempts += 1
        self.revoked_tokens.append(grant.token)


class LocalAuthorizationGateway:
    """Provides an explicitly controlled decision for the pending grant."""

    def __init__(self) -> None:
        self.calls = 0
        self.wait_entered = asyncio.Event()
        self._decision: asyncio.Future[str] | None = None

    async def await_decision(self, grant: PendingGrant) -> str:
        self.calls += 1
        self._decision = asyncio.get_running_loop().create_future()
        self.wait_entered.set()
        return await self._decision

    def authorize(self, identity: str = "alice") -> None:
        if self._decision is None:
            raise RuntimeError("Authorization was not awaited")
        self._decision.set_result(identity)

    def fail(self, error: Exception) -> None:
        if self._decision is None:
            raise RuntimeError("Authorization was not awaited")
        self._decision.set_exception(error)


class LocalIdentityWriter:
    """Writes the identity into the session behind a controllable gate."""

    def __init__(self) -> None:
        self.commits = 0
        self.wait_entered = asyncio.Event()
        self.release = asyncio.Event()

    async def commit(self, session_: SessionMixin, identity: str) -> None:
        self.wait_entered.set()
        await self.release.wait()
        session_["user"] = identity
        self.commits += 1


class AsyncAuthExtension:
    """The extension under contract test.

    ``revoke_pending=False`` intentionally produces a leak variant: cleanup
    still runs, but the pending authorization is never revoked.
    """

    def __init__(
        self,
        grants: LocalGrantStore,
        gateway: LocalAuthorizationGateway,
        identities: LocalIdentityWriter,
        *,
        revoke_pending: bool = True,
    ) -> None:
        self.grants = grants
        self.gateway = gateway
        self.identities = identities
        self.revoke_pending = revoke_pending
        self.cleanup_calls = 0
        self.phase = "idle"

    def init_app(self, app: Quart) -> None:
        app.extensions["async_auth"] = self

    async def authenticate(self) -> None:
        grant: PendingGrant | None = None
        try:
            self.phase = PHASE_ESTABLISHING
            grant = self.grants.start(request.path)
            await grant.activate()

            self.phase = PHASE_WAITING
            identity = await self.gateway.await_decision(grant)

            self.phase = PHASE_WRITING
            await self.identities.commit(session, identity)
            self.phase = "authenticated"
        finally:
            self.cleanup_calls += 1
            if grant is not None and self.revoke_pending:
                await grant.revoke()


@dataclass(frozen=True)
class AuthReport:
    """A snapshot of the observable effects of one authentication attempt."""

    phase: str
    status_code: int | None
    identity: str | None
    established_tokens: tuple[str, ...]
    revoked_tokens: tuple[str, ...]
    identity_commits: int
    cleanup_calls: int
    revocations: int
    teardown_calls: int
    teardown_exception: BaseException | None


class AuthHarness:
    """Builds a Quart app around the extension and its local stand-ins."""

    def __init__(self, *, revoke_pending: bool = True) -> None:
        self.app = Quart(__name__)
        self.app.secret_key = "auth-cancellation-contract"
        self.grants = LocalGrantStore()
        self.gateway = LocalAuthorizationGateway()
        self.identities = LocalIdentityWriter()
        self.auth = AsyncAuthExtension(
            self.grants,
            self.gateway,
            self.identities,
            revoke_pending=revoke_pending,
        )
        self.auth.init_app(self.app)
        self.teardowns: list[BaseException | None] = []

        @self.app.teardown_request
        async def record_teardown(exc: BaseException | None = None) -> None:
            self.teardowns.append(exc)

        @self.app.errorhandler(AuthorizationServiceError)
        async def handle_authorization_error(
            error: AuthorizationServiceError,
        ) -> ResponseReturnValue:
            return "authorization unavailable", 503

        @self.app.route("/login")
        async def login() -> str:
            await self.auth.authenticate()
            return "authenticated"

        @self.app.route("/whoami")
        async def whoami() -> str:
            user = session.get("user")
            return user if isinstance(user, str) else "anonymous"

        self.client = self.app.test_client()

    async def _advance_to(self, phase: str) -> None:
        await self.grants.activation_entered.wait()
        if phase == PHASE_ESTABLISHING:
            return
        self.grants.activation_release.set()
        await self.gateway.wait_entered.wait()
        if phase == PHASE_WAITING:
            return
        self.gateway.authorize()
        await self.identities.wait_entered.wait()

    async def _report(self, phase: str, status_code: int | None) -> AuthReport:
        teardowns = tuple(self.teardowns)
        identity = await self._current_identity()
        return AuthReport(
            phase=phase,
            status_code=status_code,
            identity=identity,
            established_tokens=tuple(grant.token for grant in self.grants.started),
            revoked_tokens=tuple(self.grants.revoked_tokens),
            identity_commits=self.identities.commits,
            cleanup_calls=self.auth.cleanup_calls,
            revocations=self.grants.revoke_attempts,
            teardown_calls=len(teardowns),
            teardown_exception=teardowns[0] if len(teardowns) == 1 else None,
        )

    async def _current_identity(self) -> str | None:
        response = await self.client.get("/whoami")
        identity = await response.get_data(as_text=True)
        return None if identity == "anonymous" else identity

    async def cancel_at(self, phase: str) -> AuthReport:
        async with self.client.request("/login") as connection:
            await self._advance_to(phase)
            await connection.disconnect()
        return await self._report(phase, None)

    async def complete(self) -> AuthReport:
        task = asyncio.create_task(self.client.get("/login"))
        await self._advance_to(PHASE_WRITING)
        self.identities.release.set()
        response = await task
        return await self._report("authenticated", response.status_code)

    async def fail_authorization(self, error: Exception) -> AuthReport:
        task = asyncio.create_task(self.client.get("/login"))
        await self.grants.activation_entered.wait()
        self.grants.activation_release.set()
        await self.gateway.wait_entered.wait()
        self.gateway.fail(error)
        response = await task
        return await self._report(PHASE_WAITING, response.status_code)


def assert_cancellation_contract(report: AuthReport, phase: str) -> None:
    assert report.phase == phase
    assert report.status_code is None
    assert report.identity is None, "A cancelled request must not grant an identity"
    assert report.identity_commits == 0, "Identity must not be written to the session"
    assert (
        len(report.established_tokens) == 1
    ), "Exactly one temporary authorization must be established"
    assert report.cleanup_calls == 1, "Cleanup must run exactly once per request"
    assert (
        report.revocations == 1
    ), "Temporary authorization must be revoked exactly once"
    assert (
        report.revoked_tokens == report.established_tokens
    ), "The established temporary authorization must be the one revoked"
    assert report.teardown_calls == 1, "Request teardown must run exactly once"
    assert isinstance(
        report.teardown_exception, asyncio.CancelledError
    ), "Teardown must observe the cancellation"


@pytest.mark.parametrize(
    "phase",
    PHASES,
    ids=["establishing-grant", "waiting-for-authorization", "writing-identity"],
)
async def test_cancellation_contract(phase: str) -> None:
    report = await AuthHarness().cancel_at(phase)
    assert_cancellation_contract(report, phase)


async def test_successful_authentication_writes_identity_and_reclaims_grant() -> None:
    report = await AuthHarness().complete()
    assert report.status_code == 200
    assert report.identity == "alice"
    assert report.identity_commits == 1
    assert report.revoked_tokens == report.established_tokens
    assert report.cleanup_calls == 1
    assert report.revocations == 1
    assert report.teardown_calls == 1
    assert report.teardown_exception is None


async def test_authorization_error_is_handled_and_reclaims_grant() -> None:
    report = await AuthHarness().fail_authorization(
        AuthorizationServiceError("decision backend unavailable")
    )
    assert report.status_code == 503
    assert report.identity is None
    assert report.identity_commits == 0
    assert report.revoked_tokens == report.established_tokens
    assert report.cleanup_calls == 1
    assert report.revocations == 1
    assert report.teardown_calls == 1
    assert report.teardown_exception is None


@pytest.mark.parametrize(
    "phase",
    PHASES,
    ids=["establishing-grant", "waiting-for-authorization", "writing-identity"],
)
async def test_contract_detects_variant_that_skips_revocation(phase: str) -> None:
    report = await AuthHarness(revoke_pending=False).cancel_at(phase)
    assert report.cleanup_calls == 1
    assert report.revocations == 0
    with pytest.raises(AssertionError, match="revoked exactly once"):
        assert_cancellation_contract(report, phase)
