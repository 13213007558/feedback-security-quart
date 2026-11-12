from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import Any
from typing import TYPE_CHECKING

from flask.templating import (
    DispatchingJinjaLoader as DispatchingJinjaLoader,  # noqa: F401
)
from jinja2 import Environment as BaseEnvironment
from jinja2 import Template

from .ctx import has_app_context
from .ctx import has_request_context
from .globals import app_ctx
from .globals import current_app
from .helpers import stream_with_context
from .render_scope import close_render_scope
from .render_scope import open_render_scope
from .signals import before_render_template
from .signals import template_rendered

if TYPE_CHECKING:
    from .app import Quart  # noqa
    from .ctx import _AppCtxGlobals
    from .sessions import SessionMixin
    from .wrappers import Request


class Environment(BaseEnvironment):
    """Quart specific Jinja Environment.

    This changes the default Jinja loader to use the
    DispatchingJinjaLoader, and enables async Jinja by default.
    """

    def __init__(self, app: Quart, **options: Any) -> None:
        """Create a Quart specific Jinja Environment.

        Arguments:
            app: The Quart app to bind to.
            options: The standard Jinja Environment options.
        """
        if "loader" not in options:
            options["loader"] = app.create_global_jinja_loader()
        options["enable_async"] = True
        super().__init__(**options)


async def render_template(
    template_name_or_list: str | list[str], **context: Any
) -> str:
    """Render the template with the context given.

    Arguments:
        template_name_or_list: Template name to render of a list of
            possible template names.
        context: The variables to pass to the template.
    """
    ctx = app_ctx._get_current_object()
    app = current_app._get_current_object()
    await app.update_template_context(ctx, context)
    template = app.jinja_env.get_or_select_template(  # type: ignore
        template_name_or_list
    )
    return await _render(template, context, app)


async def render_template_string(source: str, **context: Any) -> str:
    """Render the template source with the context given.

    Arguments:
        source: The template source code.
        context: The variables to pass to the template.
    """
    ctx = app_ctx._get_current_object()
    app = current_app._get_current_object()
    await app.update_template_context(ctx, context)
    template = app.jinja_env.from_string(source)
    return await _render(template, context, app)


async def _render(template: Template, context: dict, app: Quart) -> str:
    scope = await open_render_scope(
        context, template=template, checker=app.render_scope_checker
    )
    try:
        with scope.window() if scope is not None else nullcontext():
            await before_render_template.send_async(
                app,
                _sync_wrapper=app.ensure_async,  # type: ignore[arg-type]
                template=template,
                context=context,
            )
            rendered_template = await template.render_async(context)
            await template_rendered.send_async(
                app,
                _sync_wrapper=app.ensure_async,  # type: ignore[arg-type]
                template=template,
                context=context,
            )
    except BaseException as error:
        await close_render_scope(scope, error)
        raise
    else:
        await close_render_scope(scope)
    return rendered_template


async def _default_template_ctx_processor() -> dict[str, Any]:
    context: dict[str, _AppCtxGlobals | Request | SessionMixin] = {}
    ctx = app_ctx._get_current_object()
    if has_app_context():
        context["g"] = ctx.g
    if has_request_context():
        context["request"] = ctx.request
        context["session"] = ctx.session
    return context


async def stream_template(
    template_name_or_list: str | Template | list[str | Template], **context: Any
) -> AsyncIterator[str]:
    """Render a template by name with the given context as a stream.

    This returns an iterator of strings, which can be used as a
    streaming response from a view.

    Arguments:
        template_name_or_list: The name of the template to render. If a
            list is given, the first name to exist will be rendered.
        context: The variables to make available in the template.
    """
    ctx = app_ctx._get_current_object()
    app = current_app._get_current_object()
    await app.update_template_context(ctx, context)
    template = app.jinja_env.get_or_select_template(template_name_or_list)
    return await _stream(app, template, context)


async def stream_template_string(source: str, **context: Any) -> AsyncIterator[str]:
    """Render a template from the given source with the *context* as a stream.

    This returns an iterator of strings, which can
    be used as a streaming response from a view.

    Arguments:
        source: The source code of the template to render.
        context: The variables to make available in the template.
    """
    ctx = app_ctx._get_current_object()
    app = current_app._get_current_object()
    await app.update_template_context(ctx, context)
    template = app.jinja_env.from_string(source)
    return await _stream(app, template, context)


async def _stream(
    app: Quart, template: Template, context: dict[str, Any]
) -> AsyncIterator[str]:
    scope = await open_render_scope(
        context, template=template, checker=app.render_scope_checker
    )
    try:
        with scope.window() if scope is not None else nullcontext():
            await before_render_template.send_async(
                app,
                _sync_wrapper=app.ensure_async,  # type: ignore[arg-type]
                template=template,
                context=context,
            )
    except BaseException as error:
        await close_render_scope(scope, error)
        raise

    generator = template.generate_async(context)

    async def fail(error: BaseException) -> None:
        # Release before the error propagates so release failures are
        # attached to the template error rather than masking it.
        try:
            await generator.aclose()
        except BaseException as close_error:
            error.add_note(f"Error closing template stream: {close_error!r}")
        # A GeneratorExit is the caller deliberately closing the stream;
        # release failures should surface on their own rather than as
        # notes on an exception that is about to be discarded.
        exc_value = None if isinstance(error, GeneratorExit) else error
        await close_render_scope(scope, exc_value)

    async def generate() -> AsyncIterator[str]:
        try:
            while True:
                # Async generators share the caller's context, so the
                # window must close before the chunk is returned to the
                # caller. Otherwise a captured access object would stay
                # usable while the stream is paused between chunks.
                chunk: str | None = None
                with scope.window() if scope is not None else nullcontext():
                    try:
                        chunk = await generator.__anext__()
                    except StopAsyncIteration:
                        await template_rendered.send_async(
                            app,
                            _sync_wrapper=app.ensure_async,  # type: ignore[arg-type]
                            template=template,
                            context=context,
                        )
                if chunk is None:
                    break
                yield chunk
        except BaseException as error:
            await fail(error)
            raise
        finally:
            # Release on normal exhaustion too, and close Jinja's
            # generator so its own cleanup runs now rather than waiting
            # for garbage collection. Closing twice is harmless.
            await close_render_scope(scope)

    # If a request context is active, keep it while generating.
    if has_request_context():
        return stream_with_context(generate)()
    else:
        return generate()
