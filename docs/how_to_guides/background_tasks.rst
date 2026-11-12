.. _background_tasks:

Background tasks
================

If you have a task to perform where the outcome or result isn't
required you can utilise a background task to run it. Background tasks
run concurrently with the route handlers etc, i.e. in the
background. Background tasks are very useful when they contain actions
that take a lot of time to complete, as they allow a response to be
sent to the client whilst the task itself is carried out. Equally some
tasks just don't need to be completed before the response is sent and
instead can be done in the background.

Background tasks in Quart are created via the ``add_background_task``
method:

.. code-block:: python

    async def background_task():
        ...

    @app.route('/jobs/', methods=['POST'])
    async def create_job():
        app.add_background_task(background_task)
        return 'Success'

    @app.before_serving
    async def startup():
        app.add_background_task(background_task)


The background tasks will have access to the app context. The tasks
will be awaited during shutdown to ensure they complete before the app
shuts down. If your task does not complete within the config
``BACKGROUND_TASK_SHUTDOWN_TIMEOUT`` it will be cancelled.

Note ``BACKGROUND_TASK_SHUTDOWN_TIMEOUT`` should ideally be less than
any server shutdown timeout.

Synchronous background tasks are supported and will run in a separate
thread.

Per-account authorization quotas
---------------------------------

A background task can be made to borrow an account's authorization
resource (for example a signing service used when sending email
notifications) via a :class:`~quart.QuotaService`. Attach the service
to the app and pass ``account_id`` when scheduling the task:

.. code-block:: python

    from quart import QuotaService, Quart

    app = Quart(__name__)
    # Account "a" may hold two leases at once, account "b" one.
    QuotaService({"a": 2, "b": 1}, app=app)

    @app.route('/notify/<account_id>')
    async def notify(account_id):
        app.add_background_task(send_email, account_id=account_id)
        return 'Accepted'

The rules are:

- A task only takes the account's permission when it actually starts
  running, not when it is scheduled. Tasks that exceed the account
  limit wait in a FIFO queue until a slot frees.
- Each account is limited independently, so a busy account never blocks
  the others.
- The permission is always released when the task finishes, raises, or
  is cancelled, so the in-use count cannot leak.
- Once the app starts shutting down the quota service closes: new
  tasks are rejected with :class:`~quart.ServiceClosedError`, and tasks
  still waiting for a slot are revoked. Tasks already running keep
  their lease and are awaited as usual.
- An admitted task belongs to the application, not to the request that
  scheduled it. Cancelling or disconnecting that request does not
  cancel the task.

Tasks scheduled without an ``account_id`` (or while no
``QuotaService`` is attached) run unconstrained as before.

.. warning::

    As Quart is based on asyncio it will run on a single execution and
    switch between tasks as they become blocked on waiting on IO, if a
    task does not need to wait on IO it will instead block the event
    loop and Quart could become unresponsive. Additionally the task
    will consume the same CPU resources as the server and hence could
    slow the server.


Testing background tasks
------------------------

To ensure that background tasks complete in tests utilise the
``test_app`` context manager. This will wait for any background
tasks to complete before allowing the test to continue:

.. code-block:: python

    async def test_tasks_complete():
        async with app.test_app():
            app.add_background_task(...)
        # Background task has completed here
        assert task_has_done_something

Note when testing an app the ``test_client`` usage should be within
the ``test_app`` context block.

The background task coroutine function can be tested by creating an
app context and await the function,

.. code-block:: python

    async def test_background_task():
        async with app.app_context():
            await background_task()
        assert something_to_test
