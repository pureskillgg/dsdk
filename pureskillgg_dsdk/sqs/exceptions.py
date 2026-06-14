class DeleteMessage(Exception):
    """Raised by a handler to force-delete (ack) the current SQS message.

    Use this when a message should be removed from the queue even though the
    handler did not return a truthy confirmation -- e.g. a message that is
    permanently unprocessable and must not be redriven.
    """
