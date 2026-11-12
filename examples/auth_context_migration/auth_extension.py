"""Migrated auth extension for the unified-AppContext Quart snapshot.

Caching model (two levels, deliberately split):

- L1 ``g._auth_identity``: per-context memo. Dies with the context, and
  ``AppContext.copy()`` creates a fresh ``g``, so copied contexts never
  inherit a stale memo.
- L2 ``transport._auth_cache`` / ``transport._auth_revoked``: stored on the
  *shared* request/websocket object, which ``AppContext.copy()`` passes by
  reference. Nested pushes, copied contexts and async subtasks therefore
  share one authoritative resolution and one revocation flag.

Lookup checks the L2 revocation flag *before* any cache, so an invalidated
resource can never be re-activated by an inner (copied-context) cache miss.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from quart import g
from quart import has_request_context
from quart import has_websocket_context
from quart import Quart
from quart import request
from quart import websocket

_L1_KEY = "_auth_identity"


@dataclass(frozen=True)
class Identity:
    account_id: str  # 账号：已认证主体（来自凭据）
    resource_owner_id: str  # 授权资源的拥有者（来自目标资源）
    token_id: str


def _shared_transport() -> Any | None:
    """The request/websocket object shared by this context and its copies."""
    if has_request_context():
        return request._get_current_object()
    if has_websocket_context():
        return websocket._get_current_object()
    return None


class AuthExtension:
    def __init__(self, app: Quart | None = None) -> None:
        self.load_count = 0  # backend hits, asserted by the demo
        self.teardowns: list[tuple[str, BaseException | None]] = []
        self._revoked_tokens: set[str] = set()
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Quart) -> None:
        # One hook covers request AND websocket contexts in this snapshot,
        # because both are the same AppContext and share one pop path.
        app.teardown_appcontext(self._on_teardown)

    async def _on_teardown(self, exc: BaseException | None) -> None:
        if has_request_context():
            kind = "request"
        elif has_websocket_context():
            kind = "websocket"
        else:
            kind = "app"
        self.teardowns.append((kind, exc))
        # L1 (g) is discarded with the context; L2 lives on the transport
        # object and is garbage-collected with it. Revocation must survive
        # teardown, so it is intentionally NOT cleared here.

    async def _load_from_backend(self, transport: Any) -> Identity | None:
        """Simulated expensive lookup. Returns None for revoked tokens."""
        self.load_count += 1
        await asyncio.sleep(0)
        auth_header = transport.headers.get("Authorization", "")
        token = auth_header.removeprefix("Bearer ").strip()
        if not token or token in self._revoked_tokens:
            return None
        owner = transport.args.get("owner", token)
        return Identity(
            account_id=f"account-of-{token}",
            resource_owner_id=f"owner-{owner}",
            token_id=token,
        )

    async def current_identity(self) -> Identity | None:
        transport = _shared_transport()
        if transport is None:
            raise RuntimeError("auth: no request or websocket context active")

        # Revocation wins over every cache, including other contexts' L1s.
        if getattr(transport, "_auth_revoked", False):
            return None

        if _L1_KEY in g:  # L1: this context only
            return g.get(_L1_KEY)

        if hasattr(transport, "_auth_cache"):  # L2: shared with copies
            identity = transport._auth_cache
        else:
            identity = await self._load_from_backend(transport)
            transport._auth_cache = identity

        g._auth_identity = identity
        return identity

    async def revoke(self) -> None:
        """Invalidate the current credential for the whole request/websocket.

        Writes to the shared transport object, so nested contexts, copied
        contexts and parallel subtasks all observe it immediately.
        """
        transport = _shared_transport()
        if transport is None:
            raise RuntimeError("auth: no request or websocket context active")
        identity = getattr(transport, "_auth_cache", None)
        if identity is not None:
            self._revoked_tokens.add(identity.token_id)
        transport._auth_revoked = True
        transport._auth_cache = None
        g.pop(_L1_KEY, None)  # drop this context's L1; others are guarded by the flag
