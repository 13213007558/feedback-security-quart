.. _file_upload_admission:

Controlling file upload admission and staging
=============================================

A ``multipart/form-data`` request can mix ordinary attachments with
sensitive material, such as identity documents, that require extra
permissions. Quart lets you plug in an **admission policy** that decides
how each file part is staged *after its headers have been parsed but
before any of its bytes are written to a temporary file*. Combined with
per-category byte quotas this lets you refuse sensitive parts without
ever touching disk and cap how much data each category may spool.

The decision interface
----------------------

Implement :class:`~quart.staging.FieldAdmissionPolicy`. It receives an
:class:`~quart.staging.AdmissionContext` describing the current part and
returns a :class:`~quart.staging.StagingDecision`. Policies may be
synchronous or asynchronous; coroutine results are awaited automatically.

.. code-block:: python

    from quart import AdmissionContext, Request, StagingDecision


    class UploadAdmissionPolicy:
        def __call__(self, context: AdmissionContext) -> StagingDecision:
            if context.name == "id_doc":
                # Identity material needs an elevated scope this request
                # does not have. Its bytes are consumed but never stored.
                return StagingDecision.discard("missing identity scope")

            return StagingDecision.spool_decision("general")

The context exposes the part ``name``, ``filename``, multipart
``headers``, the declared ``content_length`` (when present), and the
regular form fields already parsed before this part in
``parsed_fields``. Because multipart field ordering is not guaranteed, a
policy must only rely on ``parsed_fields`` that are known to be sent
first, or on the part's own headers.

Wiring the policy into requests
-------------------------------

Attach the policy to a :class:`~quart.wrappers.request.Request` subclass
and point the application at it.

.. code-block:: python

    class GuardedRequest(Request):
        field_admission_policy = UploadAdmissionPolicy()


    app = Quart(__name__)
    app.request_class = GuardedRequest

Limiting staged bytes per category
----------------------------------

A decision names a staging ``category``. Set ``FORM_SPOOL_QUOTA`` (or the
request's ``spool_quota``) to the maximum number of bytes that may be
spooled for each category.

.. code-block:: python

    app.config["FORM_SPOOL_QUOTA"] = {
        "general": 5 * 1024 * 1024,
        "sensitive": 256 * 1024,
    }

The quota is checked both against the part's declared length when it
opens a stream and against every chunk before it is written, so an
over-quota part raises :class:`~quart.staging.StagingQuotaExceeded`
before the offending bytes reach disk. Categories without an entry are
unlimited.

Failure and cleanup rules
-------------------------

- A refused part (``StagingDecision.discard``) never reaches the stream
  factory, so no temporary file is created; its bytes are read to advance
  the parser and then dropped.
- :class:`~quart.staging.StagingQuotaExceeded` is both a
  :class:`ValueError` and a
  :class:`~werkzeug.exceptions.RequestEntityTooLarge` (HTTP ``413``).
- On any failure -- refusal, quota breach, malformed body, or client
  disconnect -- every stream already opened for the request is closed in
  reverse opening order. A successful parse transfers ownership of the
  open streams to the returned
  :class:`~quart.datastructures.FileStorage` objects instead.
- The form parser runs in ``silent`` mode by default, so a
  :class:`ValueError` results in empty form and file collections. Set the
  parser to ``silent=False`` via a custom
  :class:`~quart.formparser.FormDataParser` to let the exception
  propagate, or register an error handler for
  :class:`~quart.staging.StagingQuotaExceeded` to return a ``413``
  response.

.. code-block:: python

    from quart import Quart
    from quart.formparser import FormDataParser
    from quart import StagingQuotaExceeded


    class StrictParser(FormDataParser):
        def __init__(self, **kwargs):
            kwargs["silent"] = False
            super().__init__(**kwargs)


    app = Quart(__name__)

    @app.errorhandler(StagingQuotaExceeded)
    async def handle_quota(error):
        return {"error": "upload quota exceeded"}, 413
