import abc

import rapidjson


class AbstractMessageTranslator(abc.ABC):
    """Base class for SQS message translators.

    A translator converts a raw SQS message dict into a
    ``{"content": <payload>, "metadata": <fields>}`` mapping. ``content`` is
    passed as the first argument to the consumer's handler and ``metadata`` as
    the second. Implementations must NOT mutate the input ``message`` -- the
    consumer hands the original raw message to ``error_handler`` when delivery
    fails, so it must stay intact (e.g. for poison-message debugging).
    """

    @abc.abstractmethod
    def translate(self, message):
        raise NotImplementedError


class SqsJsonMessageTranslator(AbstractMessageTranslator):
    """Parse the JSON ``Body`` of an SQS message into ``content``.

    The remaining message fields (``MessageId``, ``MessageAttributes``,
    ``ReceiptHandle``, system ``Attributes``, ...) are returned as ``metadata``
    (a copy excluding ``Body``); the input ``message`` is left unmodified. A
    message with no ``Body`` yields ``content=None``, which the consumer routes
    to ``error_handler`` rather than dispatching.
    """

    def translate(self, message):
        body = message.get("Body")
        content = rapidjson.loads(body) if body is not None else None
        metadata = {key: value for key, value in message.items() if key != "Body"}
        return {"content": content, "metadata": metadata}
