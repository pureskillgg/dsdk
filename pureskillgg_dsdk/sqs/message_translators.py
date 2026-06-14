import abc

import rapidjson


class AbstractMessageTranslator(abc.ABC):
    """Base class for SQS message translators.

    A translator converts a raw SQS message dict into a
    ``{"content": <payload>, "metadata": <fields>}`` mapping. ``content`` is
    passed as the first argument to the consumer's handler and ``metadata`` as
    the second.
    """

    @abc.abstractmethod
    def translate(self, message):
        raise NotImplementedError


class SqsJsonMessageTranslator(AbstractMessageTranslator):
    """Parse the JSON ``Body`` of an SQS message into ``content``.

    The remaining message fields (``MessageId``, ``MessageAttributes``,
    ``ReceiptHandle``, system ``Attributes``, ...) are returned as
    ``metadata``. A message with no ``Body`` yields ``content=None``, which the
    consumer ignores (the message is left on the queue).
    """

    def translate(self, message):
        body = message.pop("Body", None)
        content = rapidjson.loads(body) if body is not None else None
        return {"content": content, "metadata": message}
