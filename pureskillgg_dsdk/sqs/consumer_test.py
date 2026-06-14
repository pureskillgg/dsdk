import asyncio
import os

import rapidjson
from structlog import get_logger

from .consumer import SqsConsumer, _make_default_client_factory
from .exceptions import DeleteMessage
from .message_translators import AbstractMessageTranslator, SqsJsonMessageTranslator


def make_message(message_id, body, attributes=None):
    return {
        "MessageId": message_id,
        "ReceiptHandle": f"rh-{message_id}",
        "Body": rapidjson.dumps(body),
        "MessageAttributes": attributes or {},
    }


class FakeSqsClient:
    def __init__(
        self, messages, queue_url="https://sqs.test/0/queue", block_when_empty=True
    ):
        self._messages = list(messages)
        self._queue_url = queue_url
        self._block_when_empty = block_when_empty
        self.deleted = []
        self.received_kwargs = []
        self.get_queue_url_names = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def get_queue_url(self, **kwargs):
        self.get_queue_url_names.append(kwargs["QueueName"])
        return {"QueueUrl": self._queue_url}

    async def receive_message(self, **kwargs):
        self.received_kwargs.append(kwargs)
        if self._messages:
            count = kwargs.get("MaxNumberOfMessages", 1)
            batch = self._messages[:count]
            self._messages = self._messages[count:]
            return {"Messages": batch}
        if self._block_when_empty:
            await asyncio.sleep(3600)  # simulate an empty long-poll until cancelled
        await asyncio.sleep(0)
        return {}

    async def delete_message(self, **kwargs):
        self.deleted.append(kwargs["ReceiptHandle"])


def factory_for(client):
    def client_factory():
        return client

    return client_factory


def run_until(consumer, predicate, timeout=5):
    async def _run():
        task = asyncio.create_task(consumer.consume())
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            if task.done():
                await task  # surface a consumer error instead of hanging
                break
            if asyncio.get_running_loop().time() > deadline:
                task.cancel()
                raise TimeoutError("consumer did not satisfy predicate in time")
            await asyncio.sleep(0.005)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())


def test_confirmed_message_is_deleted():
    received = []

    async def handler(content, metadata):
        received.append((content, metadata))
        return True

    fake = FakeSqsClient(
        [make_message("1", {"job": "a"}, {"reqId": {"StringValue": "r1"}})]
    )
    consumer = SqsConsumer(
        queue="q", handler=handler, log=get_logger(), client_factory=factory_for(fake)
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)

    assert fake.deleted == ["rh-1"]
    content, metadata = received[0]
    assert content == {"job": "a"}
    assert metadata["MessageId"] == "1"
    assert metadata["MessageAttributes"]["reqId"]["StringValue"] == "r1"
    assert "Body" not in metadata


def test_unconfirmed_message_is_not_deleted():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        return False

    fake = FakeSqsClient([make_message("1", {"x": 1})])
    consumer = SqsConsumer(
        queue="q", handler=handler, log=get_logger(), client_factory=factory_for(fake)
    )
    run_until(consumer, lambda: len(fake.received_kwargs) >= 2)
    assert not fake.deleted


def test_delete_message_exception_acks():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        raise DeleteMessage()

    fake = FakeSqsClient([make_message("1", {"x": 1})])
    consumer = SqsConsumer(
        queue="q", handler=handler, log=get_logger(), client_factory=factory_for(fake)
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)
    assert fake.deleted == ["rh-1"]


def test_handler_error_routed_to_error_handler():
    errors = []

    async def handler(content, metadata):  # pylint: disable=unused-argument
        raise RuntimeError("boom")

    async def error_handler(exc_info, message):
        errors.append((exc_info[0], message["MessageId"]))
        return True

    fake = FakeSqsClient([make_message("1", {"x": 1})])
    consumer = SqsConsumer(
        queue="q",
        handler=handler,
        log=get_logger(),
        error_handler=error_handler,
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)
    assert fake.deleted == ["rh-1"]
    assert errors[0][0] is RuntimeError
    assert errors[0][1] == "1"


def test_default_error_handler_leaves_message():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        raise RuntimeError("boom")

    fake = FakeSqsClient([make_message("1", {"x": 1})])
    consumer = SqsConsumer(
        queue="q", handler=handler, log=get_logger(), client_factory=factory_for(fake)
    )
    run_until(consumer, lambda: len(fake.received_kwargs) >= 2)
    assert not fake.deleted


def test_resolves_queue_url_from_name():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        return True

    fake = FakeSqsClient([make_message("1", {})])
    consumer = SqsConsumer(
        queue="my-queue",
        handler=handler,
        log=get_logger(),
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)
    assert fake.get_queue_url_names == ["my-queue"]


def test_uses_queue_url_directly_without_lookup():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        return True

    fake = FakeSqsClient([make_message("1", {})])
    consumer = SqsConsumer(
        queue="https://sqs.us-east-1.amazonaws.com/0/q",
        handler=handler,
        log=get_logger(),
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)
    assert not fake.get_queue_url_names


def test_empty_body_routed_to_error_handler():
    called = []
    errors = []

    async def handler(content, metadata):  # pylint: disable=unused-argument
        called.append(content)
        return True

    async def error_handler(exc_info, message):  # pylint: disable=unused-argument
        errors.append(message["MessageId"])
        return False

    message = {"MessageId": "1", "ReceiptHandle": "rh-1"}  # no Body
    fake = FakeSqsClient([message])
    consumer = SqsConsumer(
        queue="q",
        handler=handler,
        log=get_logger(),
        error_handler=error_handler,
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: errors)
    assert errors == ["1"]
    assert not called  # handler is never invoked for an empty message
    assert not fake.deleted  # error_handler returned False -> left for redrive


def test_invalid_json_body_routed_to_error_handler():
    errors = []

    async def handler(content, metadata):  # pylint: disable=unused-argument
        return True

    async def error_handler(exc_info, message):  # pylint: disable=unused-argument
        errors.append(message["MessageId"])
        return True  # ack the poison message

    message = {"MessageId": "1", "ReceiptHandle": "rh-1", "Body": "not-json{"}
    fake = FakeSqsClient([message])
    consumer = SqsConsumer(
        queue="q",
        handler=handler,
        log=get_logger(),
        error_handler=error_handler,
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)
    assert errors == ["1"]
    assert fake.deleted == ["rh-1"]


def test_default_client_factory_builds_aiobotocore_client():
    # Proves the real aiobotocore seam: get_session().create_client("sqs") is
    # called correctly and yields a client exposing the methods the consumer
    # uses. Offline -- dummy creds + metadata disabled, no API call is made.
    env = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_EC2_METADATA_DISABLED": "true",
    }

    async def _run():
        factory = _make_default_client_factory("us-east-1")
        async with factory() as client:
            assert hasattr(client, "receive_message")
            assert hasattr(client, "delete_message")
            assert hasattr(client, "get_queue_url")

    previous = {key: os.environ.get(key) for key in env}
    os.environ.update(env)
    try:
        asyncio.run(_run())
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_passes_receive_options_over_defaults():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        return True

    fake = FakeSqsClient([make_message("1", {})])
    consumer = SqsConsumer(
        queue="q",
        handler=handler,
        log=get_logger(),
        options={"WaitTimeSeconds": 5, "MaxNumberOfMessages": 3},
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)
    kwargs = fake.received_kwargs[0]
    assert kwargs["WaitTimeSeconds"] == 5
    assert kwargs["MaxNumberOfMessages"] == 3
    assert kwargs["MessageAttributeNames"] == ["All"]


def test_concurrency_processes_all_messages():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        await asyncio.sleep(0.01)
        return True

    messages = [make_message(str(i), {"i": i}) for i in range(6)]
    fake = FakeSqsClient(messages)
    consumer = SqsConsumer(
        queue="q",
        handler=handler,
        log=get_logger(),
        concurrency=3,
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: len(fake.deleted) == 6)
    assert sorted(fake.deleted) == sorted(f"rh-{i}" for i in range(6))


def test_custom_translator():
    seen = []

    class PlainTranslator(AbstractMessageTranslator):
        def translate(self, message):
            return {
                "content": message.get("Body"),
                "metadata": {"id": message["MessageId"]},
            }

    async def handler(content, metadata):
        seen.append((content, metadata))
        return True

    fake = FakeSqsClient([{"MessageId": "1", "ReceiptHandle": "rh-1", "Body": "raw"}])
    consumer = SqsConsumer(
        queue="q",
        handler=handler,
        log=get_logger(),
        message_translator=PlainTranslator(),
        client_factory=factory_for(fake),
    )
    run_until(consumer, lambda: len(fake.deleted) == 1)
    assert seen[0][0] == "raw"
    assert seen[0][1] == {"id": "1"}


def test_json_translator_unit():
    translator = SqsJsonMessageTranslator()
    message = {
        "MessageId": "1",
        "ReceiptHandle": "rh-1",
        "Body": rapidjson.dumps({"a": 1}),
    }
    result = translator.translate(message)
    assert result["content"] == {"a": 1}
    assert result["metadata"]["MessageId"] == "1"
    assert "Body" not in result["metadata"]


def test_stop_exits_cleanly():
    async def handler(content, metadata):  # pylint: disable=unused-argument
        return True

    fake = FakeSqsClient([make_message("1", {})], block_when_empty=False)
    consumer = SqsConsumer(
        queue="q", handler=handler, log=get_logger(), client_factory=factory_for(fake)
    )

    async def _run():
        task = asyncio.create_task(consumer.consume())
        while not fake.deleted:
            await asyncio.sleep(0.005)
        consumer.stop()
        await asyncio.wait_for(task, timeout=5)

    asyncio.run(_run())
    assert fake.deleted == ["rh-1"]
