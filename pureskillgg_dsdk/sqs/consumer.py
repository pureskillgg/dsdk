import asyncio
import sys

from aiobotocore.session import get_session

from .exceptions import DeleteMessage
from .message_translators import SqsJsonMessageTranslator

DEFAULT_OPTIONS = {
    "WaitTimeSeconds": 20,
    "MaxNumberOfMessages": 1,
    "MessageSystemAttributeNames": ["All"],
    "MessageAttributeNames": ["All"],
}


async def _default_error_handler(exc_info, message):  # pylint: disable=unused-argument
    return False


def _make_default_client_factory(region_name):
    session = get_session()

    def client_factory():
        return session.create_client("sqs", region_name=region_name)

    return client_factory


class SqsConsumer:
    """Asynchronous SQS queue consumer.

    Long-polls an SQS queue and dispatches each message to an async ``handler``
    with bounded concurrency, deleting (acking) the messages the handler
    confirms and leaving the rest on the queue for its redrive policy. This
    replaces the abandoned ``loafer`` package -- whose ``aiobotocore<2``
    requirement was the single reason the workers were pinned to ``boto3``
    1.17 (2021). It is built directly on ``aiobotocore``.

    Handler contract (mirrors loafer): ``await handler(content, metadata)``;
    a truthy return deletes the message, a falsy return (or an unhandled
    exception, which is routed through ``error_handler``) leaves it on the
    queue. A handler may raise :class:`DeleteMessage` to force a delete.

    ``content`` and ``metadata`` come from ``message_translator``; the default
    :class:`SqsJsonMessageTranslator` parses the JSON ``Body`` into ``content``
    and exposes the rest of the message (``MessageId``, ``MessageAttributes``,
    ...) as ``metadata``. A message with a missing or unparseable ``Body``
    (empty ``content``) is routed through ``error_handler`` -- like a handler
    exception -- rather than dispatched, so callers can observe poison
    messages.

    Hosting: :meth:`consume` is the primitive coroutine. To run it on an
    externally-owned event loop (e.g. Tornado's ``IOLoop``), schedule it as a
    task with ``loop.create_task(consumer.consume())`` or :meth:`start` -- the
    consumer never owns or blocks the loop. :meth:`run` is a blocking
    convenience for standalone use.

    ``client_factory`` (a no-arg callable returning an async context manager
    that yields an SQS client) is injectable for testing; by default it builds
    an ``aiobotocore`` SQS client.
    """

    def __init__(
        self,
        *,
        queue,
        handler,
        log,
        message_translator=None,
        error_handler=None,
        concurrency=1,
        options=None,
        client_factory=None,
        region_name=None,
        error_backoff_seconds=1,
    ):
        self._queue = queue
        self._handler = handler
        self._log = log.bind(queue=queue)
        self._translator = message_translator or SqsJsonMessageTranslator()
        self._error_handler = error_handler or _default_error_handler
        # None mirrors loafer's _concurrency_limit/input_limit "use default" sentinel.
        self._concurrency = 1 if concurrency is None else max(1, int(concurrency))
        self._options = {**DEFAULT_OPTIONS, **(options or {})}
        self._error_backoff_seconds = error_backoff_seconds
        self._stopped = False
        self._client_factory = client_factory or _make_default_client_factory(
            region_name
        )

    def start(self, loop=None):
        """Schedule :meth:`consume` as a task on ``loop`` (or the current loop).

        Non-blocking; returns the created task. Use this when hosting under an
        externally-owned event loop that is started elsewhere -- e.g. inside a
        Tornado ``lifecycle.on_start`` before ``IOLoop.start()``, which is how
        the deployed workers run their queue consumer.
        """
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
        return loop.create_task(self.consume())

    def run(self):
        """Blocking convenience: own a fresh event loop and consume until stopped."""
        asyncio.run(self.consume())

    def stop(self):
        """Request a graceful stop: worker loops exit after their current poll.

        This is the *graceful* drain -- a worker parked in a long-poll keeps
        waiting up to ``WaitTimeSeconds`` before it notices. For an *immediate*
        stop (e.g. on a Tornado ``IOLoop`` teardown), cancel the :meth:`consume`
        task instead; cancellation interrupts the in-flight poll at once.
        """
        self._stopped = True

    async def consume(self):
        """Poll the queue and dispatch messages until cancelled or stopped."""
        async with self._client_factory() as client:
            url = await self._resolve_queue_url(client)
            self._log.info(
                "SQS Consumer: Start", queue_url=url, concurrency=self._concurrency
            )
            workers = [
                asyncio.ensure_future(self._worker(client, url))
                for _ in range(self._concurrency)
            ]
            try:
                await asyncio.gather(*workers)
            finally:
                for worker in workers:
                    worker.cancel()
                # Wait for the cancelled workers to fully unwind before the
                # `async with` closes the shared client -- otherwise their
                # in-flight calls get torn down ("Task was destroyed" noise).
                await asyncio.gather(*workers, return_exceptions=True)
                self._log.info("SQS Consumer: Stop")

    async def _resolve_queue_url(self, client):
        if "://" in self._queue:
            return self._queue
        response = await client.get_queue_url(QueueName=self._queue)
        return response["QueueUrl"]

    async def _worker(self, client, url):
        while not self._stopped:
            try:
                response = await client.receive_message(QueueUrl=url, **self._options)
                for message in response.get("Messages", []):
                    await self._handle_message(client, url, message)
            # CancelledError is a BaseException (not Exception), so cancellation
            # on shutdown propagates past this guard rather than being retried.
            except Exception:  # pylint: disable=broad-except
                self._log.exception("SQS Consumer: Receive error")
                await asyncio.sleep(self._error_backoff_seconds)

    async def _handle_message(self, client, url, message):
        receipt_handle = message.get("ReceiptHandle")
        confirm = await self._deliver(message)
        if confirm and receipt_handle is not None:
            await client.delete_message(QueueUrl=url, ReceiptHandle=receipt_handle)

    async def _deliver(self, message):
        # Translate + dispatch in one flow so that translate-stage failures
        # (a missing/unparseable Body, or empty content) are routed to the
        # error handler exactly like a handler exception -- mirroring loafer,
        # where the error handler observes every non-deliverable message.
        try:
            translated = self._translator.translate(message)
            content = translated.get("content")
            metadata = translated.get("metadata", {})
            if content is None:
                raise ValueError("SQS message has no content to dispatch")
            return bool(await self._handler(content, metadata))
        except DeleteMessage:
            self._log.info("SQS Consumer: Explicit delete")
            return True
        # CancelledError (a BaseException) is intentionally not caught here so
        # that shutdown cancellation is never routed to the error handler.
        except Exception:  # pylint: disable=broad-except
            self._log.exception("SQS Consumer: Message error")
            return bool(await self._error_handler(sys.exc_info(), message))
