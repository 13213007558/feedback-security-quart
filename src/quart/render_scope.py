from __future__ import annotations

import asyncio
import weakref
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import update_wrapper
from inspect import isawaitable
from inspect import iscoroutinefunction
from typing import Any
from typing import Generic
from typing import TypeVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from jinja2 import Template


T = TypeVar("T")


class RenderScopeError(RuntimeError):
    """Base class for errors relating to render scoped values."""


class RenderScopeClosedError(RenderScopeError):
    """Raised when a render scoped value is used outside its render."""


class RenderScopeDeniedError(RenderScopeError):
    """Raised when a render is not approved to use render scoped values."""


class ReadOnlyAccessError(RenderScopeError):
    """Raised when a render scoped access object is written to."""


@dataclass(frozen=True, slots=True)
class ScopedRenderValue:
    """Marks a template context value as scoped to a single render.

    Context processors should create these with :func:`render_scoped`
    rather than constructing this class directly.
    """

    value: Any
    release: Callable[[Any], Any] | None = None
    owner: Any = None


def render_scoped(
    value: T,
    *,
    release: Callable[[T], Any] | None = None,
    owner: Any = None,
) -> ScopedRenderValue:
    """Mark a template context value as available only for one render.

    A context processor can return the result of this function to opt a
    privileged data access object into render scoping. Quart replaces
    the marker with a read-only proxy for the render only. After the
    render completes, is closed, or raises, the proxy raises
    :class:`RenderScopeClosedError` for every further access or call.

    If the render is not approved by
    :attr:`~quart.app.Quart.render_scope_checker`, the render fails with
    :class:`RenderScopeDeniedError` before the template executes.

    Arguments:
        value: The read-only object that should be available during the
            render.
        release: An optional callable invoked with *value* when the
            render ends. If omitted, an ``aclose`` or ``close`` method on
            *value* is called if one exists. Releases run in reverse
            render scope binding order.
        owner: An optional label, such as the account identifier, that
            is included in errors. It does not grant access; access is
            granted by the render itself.
    """
    if isinstance(value, ScopedRenderValue):
        raise TypeError("This value is already render scoped")
    return ScopedRenderValue(value, release, owner)


def _label(owner: Any, name: str) -> str:
    if owner is None:
        return repr(name)
    return f"{owner!r}:{name}"


class _Method:
    """A callable that checks the owning scope before delegating."""

    __slots__ = ("__dict__", "_label", "_method", "_scope")

    def __init__(
        self, method: Callable[..., Any], scope: RenderScope, label: str
    ) -> None:
        self._method = method
        self._scope = scope
        self._label = label
        try:
            update_wrapper(self, method)
        except (AttributeError, TypeError):
            pass

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._scope.ensure_active(self._label)
        result = self._method(*args, **kwargs)
        if isawaitable(result):
            # A sync function may return an awaitable; make sure the
            # scope is still active when it is finally awaited.
            return self._await(result)
        return result

    async def _await(self, awaitable: Any) -> Any:
        self._scope.ensure_active(self._label)
        return await awaitable

    def __repr__(self) -> str:
        return f"<render scoped callable {self._method!r}>"


class _AsyncMethod(_Method):
    __slots__ = ()

    def __call__(self, *args: Any, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        self._scope.ensure_active(self._label)
        return self._await(*args, **kwargs)

    async def _await(self, *args: Any, **kwargs: Any) -> Any:
        self._scope.ensure_active(self._label)
        return await self._method(*args, **kwargs)


class ReadOnlyAccess(Generic[T]):
    """A proxy that is read-only and only usable within its render.

    Attribute access and calls are checked against the owning
    :class:`RenderScope`. Once the render has ended every further
    attribute access or call raises :class:`RenderScopeClosedError`.
    Writing through the proxy raises :class:`ReadOnlyAccessError`.

    Methods are wrapped on access, so a method captured during the render
    still fails when called afterwards. Non-callable attributes that were
    already read cannot be revoked; access objects should therefore
    expose their operations as methods rather than as data attributes.

    This proxy is intentionally not an identity wrapper: ``is`` checks
    against the wrapped object do not hold. It is a capability for a
    render, not a general proxy.
    """

    __slots__ = ("_scope", "_target", "_label")

    def __init__(self, scope: RenderScope, target: T, label: str) -> None:
        object.__setattr__(self, "_scope", scope)
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_label", label)

    def __getattribute__(self, name: str) -> Any:
        if name in {"_scope", "_target", "_label"}:
            raise AttributeError(
                f"{name!r} is not available on a read-only access object"
            )
        if name.startswith("__") and name.endswith("__"):
            # Object protocols must be discoverable by the interpreter.
            return object.__getattribute__(self, name)
        scope = object.__getattribute__(self, "_scope")
        label = object.__getattribute__(self, "_label")
        scope.ensure_active(label)
        attribute = getattr(object.__getattribute__(self, "_target"), name)
        if callable(attribute):
            if iscoroutinefunction(attribute):
                return _AsyncMethod(attribute, scope, label)
            return _Method(attribute, scope, label)
        return attribute

    def __setattr__(self, name: str, value: Any) -> None:
        raise ReadOnlyAccessError(
            "Cannot set attributes on a read-only render access object"
        )

    def __delattr__(self, name: str) -> None:
        raise ReadOnlyAccessError(
            "Cannot delete attributes on a read-only render access object"
        )

    def __getitem__(self, key: Any) -> Any:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        return object.__getattribute__(self, "_target")[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        raise ReadOnlyAccessError(
            "Cannot set items on a read-only render access object"
        )

    def __delitem__(self, key: Any) -> None:
        raise ReadOnlyAccessError(
            "Cannot delete items on a read-only render access object"
        )

    def __iter__(self) -> Any:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        return iter(object.__getattribute__(self, "_target"))

    def __len__(self) -> int:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        return len(object.__getattribute__(self, "_target"))

    def __contains__(self, item: Any) -> bool:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        return item in object.__getattribute__(self, "_target")

    def __bool__(self) -> bool:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        return bool(object.__getattribute__(self, "_target"))

    def __str__(self) -> str:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        return str(object.__getattribute__(self, "_target"))

    def __repr__(self) -> str:
        scope = object.__getattribute__(self, "_scope")
        label = object.__getattribute__(self, "_label")
        if not scope.active:
            return f"<ReadOnlyAccess {label} closed>"
        target = object.__getattribute__(self, "_target")
        return f"<ReadOnlyAccess {label} active for {target!r}>"

    def __aenter__(self) -> Any:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        target = object.__getattribute__(self, "_target")
        return target.__aenter__()

    def __aexit__(self, exc_type: Any, exc_value: Any, tb: Any) -> Any:
        scope = object.__getattribute__(self, "_scope")
        scope.ensure_active(object.__getattribute__(self, "_label"))
        target = object.__getattribute__(self, "_target")
        return target.__aexit__(exc_type, exc_value, tb)


class _ReleaseQueue:
    """Holds pending releases so a finalizer cannot retain the scope."""

    __slots__ = ("closed", "releases")

    def __init__(self) -> None:
        self.closed = False
        self.releases: list[tuple[str, Callable[[Any], Any] | None, Any]] = []


class RenderScope:
    """Owns the lifespan of the scoped values of one render.

    A scope is active while the render is running and released when the
    render completes, is closed, or raises. Releases are run in reverse
    binding order. A scope is single use: once ended it cannot be
    reopened.

    If a stream is abandoned without being exhausted or closed, a weak
    reference finalizer marks the scope closed and releases the values
    best effort when the scope is garbage collected. Explicit
    ``aclose`` (or consuming the stream) is the deterministic path.
    """

    __slots__ = (
        "__weakref__",
        "_active",
        "_closed",
        "_depth",
        "_finalizer",
        "_owner",
        "_queue",
    )

    def __init__(self) -> None:
        self._active = True
        self._closed = False
        self._depth = 0
        self._owner: Any = None
        self._queue = _ReleaseQueue()
        # The finalizer must not reference the scope itself, otherwise it
        # keeps the scope alive forever. Track only the release queue.
        self._finalizer = weakref.finalize(self, _finalize_queue, self._queue)

    @property
    def active(self) -> bool:
        return self._active

    @property
    def closed(self) -> bool:
        return self._closed

    def bind_owner(self, owner: Any) -> None:
        """Set the label used for values without their own owner."""
        self._owner = owner

    def ensure_active(self, label: str) -> None:
        # Both conditions matter: the scope must still be open, and its
        # template code must currently be executing. The depth check
        # makes a value captured from a paused stream unusable between
        # chunks, and makes a captured value useless in a later render.
        if self._closed or not self._active or self._depth <= 0:
            raise RenderScopeClosedError(
                f"Render scoped value {label} is no longer available;"
                " it can only be used while its render is executing"
            )

    @contextmanager
    def window(self) -> Iterator[None]:
        """Mark scope values usable for the enclosed template steps.

        The window is intentionally not a ``ContextVar``: async
        generators resume in the caller's context, so a context variable
        would leak the opening of one stream into a different stream
        paused in the same task. A per-scope depth resolves each proxy to
        the render that owns it.
        """
        self._ensure_open()
        self._depth += 1
        try:
            yield
        finally:
            self._depth -= 1
        self._ensure_open()

    def _ensure_open(self) -> None:
        if self._closed or not self._active:
            raise RenderScopeClosedError("The render scope has ended")

    def bind(self, name: str, scoped: ScopedRenderValue) -> ReadOnlyAccess[Any]:
        """Bind a marker to this scope and return its read-only proxy."""
        self._ensure_open()
        if not isinstance(scoped, ScopedRenderValue):
            raise TypeError("Render scoped values must be created by render_scoped")
        owner = scoped.owner if scoped.owner is not None else self._owner
        label = _label(owner, name)
        proxy: ReadOnlyAccess[Any] = ReadOnlyAccess(self, scoped.value, label)
        self._queue.releases.append((label, scoped.release, scoped.value))
        return proxy

    async def aclose(self, exc_value: BaseException | None = None) -> None:
        """End the scope, releasing values in reverse binding order.

        If *exc_value* is given, release failures are attached to it as
        notes so that the original error is not masked. Otherwise
        release failures are raised as a group.
        """
        if self._closed:
            return
        self._closed = True
        self._active = False
        self._depth = 0
        self._finalizer.detach()
        queue = self._queue
        queue.closed = True
        releases = queue.releases
        queue.releases = []
        errors: list[BaseException] = []
        for label, release, value in reversed(releases):
            try:
                await _release(release, value)
            except BaseException as error:  # noqa: PERF203
                error.add_note(f"Released render scoped value: {label}")
                errors.append(error)

        if errors:
            if exc_value is not None:
                for error in errors:
                    exc_value.add_note(
                        f"Error while releasing a render scoped value: {error!r}"
                    )
            else:
                raise BaseExceptionGroup("Errors releasing render scope", errors)

    async def __aenter__(self) -> RenderScope:
        if self._closed:
            raise RenderScopeClosedError("Cannot enter an already closed render scope")
        self._active = True
        return self

    async def __aexit__(self, exc_type: Any, exc_value: Any, tb: Any) -> None:
        await self.aclose(exc_value)


def _finalize_queue(queue: _ReleaseQueue) -> None:
    """Best effort release when an abandoned scope is collected."""
    if queue.closed:
        return
    queue.closed = True
    releases = queue.releases
    queue.releases = []
    pending: list[Any] = []
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    for _, release, value in reversed(releases):
        try:
            result = _call_release(release, value)
        except BaseException:
            continue
        if isawaitable(result):
            if loop is None:
                _run_awaitable(result)
            else:
                pending.append(result)
    if loop is not None and pending:
        loop.create_task(_await_all(pending))


def _run_awaitable(awaitable: Any) -> None:
    try:
        asyncio.run(_await_all([awaitable]))
    except BaseException:
        close = getattr(awaitable, "close", None)
        if close is not None:
            close()


async def _await_all(awaitables: list[Any]) -> None:
    for awaitable in awaitables:
        await awaitable


def _call_release(release: Callable[[Any], Any] | None, value: Any) -> Any:
    if release is not None:
        return release(value)
    close = getattr(value, "aclose", None)
    if close is None:
        close = getattr(value, "close", None)
    if close is None:
        return None
    return close()


async def _release(release: Callable[[Any], Any] | None, value: Any) -> None:
    result = _call_release(release, value)
    if isawaitable(result):
        await result


async def open_render_scope(
    context: MutableMapping[str, Any],
    *,
    template: Template,
    checker: Callable[[Template, MutableMapping[str, Any]], Any] | None = None,
) -> RenderScope | None:
    """Replace scoped markers in *context* with read-only render proxies.

    This is the internal integration point used by the rendering
    functions. Nothing is replaced when no values opted in, in which
    case ``None`` is returned and plain context values are left
    untouched. The returned scope must be closed with
    :func:`close_render_scope`.
    """
    markers = [
        (name, value)
        for name, value in context.items()
        if isinstance(value, ScopedRenderValue)
    ]
    if not markers:
        return None

    scope = RenderScope()
    scope.bind_owner(template.name)
    try:
        for name, marker in markers:
            context[name] = scope.bind(name, marker)
        if checker is not None and not await _check_approved(
            checker, template, context
        ):
            raise RenderScopeDeniedError(
                f"The render of {template.name!r} is not approved to use"
                " render scoped values"
            )
    except BaseException as error:
        await scope.aclose(error)
        raise
    return scope


async def close_render_scope(
    scope: RenderScope | None, exc_value: BaseException | None = None
) -> None:
    """Close a scope returned by :func:`open_render_scope`."""
    if scope is not None:
        await scope.aclose(exc_value)


async def _check_approved(
    checker: Callable[[Template, MutableMapping[str, Any]], Any],
    template: Template,
    context: MutableMapping[str, Any],
) -> bool:
    result = checker(template, context)
    if isawaitable(result):
        result = await result
    return bool(result)


