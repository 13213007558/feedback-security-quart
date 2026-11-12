.. _background_task_discussion:

Background tasks
================

The API for background tasks follows Sanic and Starlette by taking a
callable and arguments. However, Quart will ensure that the tasks
finish during the shutdown (unless the server times out and
cancels). This is as you'd hope in a production environment.

Errors raised in a background task are logged but otherwise ignored
allowing the app to continue - much like with request/websocket
handling errors.

A scheduled background task is owned by the application rather than
the request that created it. If that request is cancelled or the
client disconnects, the task keeps running. When a
:class:`~quart.QuotaService` is attached and the task is scheduled
with an ``account_id``, the task additionally waits for the account's
authorization quota before it runs. The permission is only taken once
the task starts, held for the duration of the task, and released on
every completion path (success, error or cancellation), so the
account counters cannot leak. Closing the service during shutdown
revokes waiting tasks and rejects any new ones.
