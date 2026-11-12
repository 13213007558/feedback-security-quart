.. _session_storage:

Session Storage
===============

It is often useful to store information relevant to a user of the app
for the duration of that usage session. For example the user may
choose to want to save a option or be remembered as logged in. This
information can either be stored client side or server side and Quart
provides a system to store the information client side via Secure
Cookie Sessions.

Secure Cookie Sessions
----------------------

Secure Cookie Sessions store the session information on the Cookie in
plain text with a signature to ensure that the information is not
altered by the client. They can be used in Quart so long as the
:attr:`~quart.app.Quart.secret_key` is set to a **secret**
value.

An example usage to store a users colour preference would be,

.. code-block:: python

    from quart import session
    ...


    @app.route('/')
    async def index():
        return await render_template(
            'index.html',
            colour=session.get('colour', 'black'),
        )

    @app.route('/colour/', methods=['POST'])
    async def set_colour():
        ...
        session['colour'] = colour
        return redirect(url_for('index'))

Permanent Sessions
------------------

The cookies used by default are not set to be permanent (deleted when
the browser's session ends) to have permanent cookies
``session.permanent`` must be ``True`` when the session is
modified. To set this as the default use this snippet,

.. code-block:: python

    @app.before_request
    def make_session_permanent():
        session.permanent = True

WebSockets
----------

Sessions can be used with WebSockets with an important caveat about
cookies. A cookie can only be set on a HTTP response, and an accepted
WebSocket connection cannot return a HTTP response. Therefore any
modifications made *before* the connection is accepted are persisted
via the acceptance response headers (ASGI spec version 2.1 or newer),
whereas modifications made *after* acceptance cannot be persisted.

To check whether a change can be persisted before making it, use the
:meth:`~quart.sessions.SessionInterface.can_save_session` capability
contract, passing None as the response once the connection is
accepted,

.. code-block:: python

    from quart import current_app, session, websocket

    @app.websocket('/ws')
    async def ws():
        await websocket.accept()
        if current_app.session_interface.can_save_session(None):
            session['colour'] = 'black'
        else:
            ...  # changes would be lost, choose another approach

If a modified session cannot be persisted a
:class:`~quart.sessions.SessionSaveError` is raised (rather than the
changes being silently lost), which can be handled via an error
handler in the usual way.
