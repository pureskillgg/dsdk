# SqsConsumer (the loafer replacement)

`pureskillgg_dsdk.sqs.SqsConsumer` is the in-house SQS consumer added in dsdk
3.0. It replaces the abandoned `loafer` package and is what the deployed
PureSkill.gg Python workers use to pull jobs off SQS. It deliberately mirrors the
loafer handler contract so worker code did not have to change.

Source: `pureskillgg_dsdk/sqs/consumer.py`,
`pureskillgg_dsdk/sqs/message_translators.py`,
`pureskillgg_dsdk/sqs/exceptions.py`.

## What it is

An asyncio / aiobotocore **long-poll** consumer with **bounded concurrency**. The
lifecycle methods are `consume()`, `start()`, `run()`, and `stop()`.

- The **queue** is passed in as the `queue` kwarg. If it has no `://` it is
  treated as a name and resolved via `get_queue_url`; otherwise it is used as a
  URL. Region is the optional `region_name` kwarg.
- No queue is created or declared by the library.

## Handler contract

The consumer calls `await handler(content, metadata)`:

- A **truthy return** acks/deletes the message.
- Raising **`DeleteMessage`** force-acks (deletes) the message — use it for a
  poison message you want gone.
- A **falsy return or any exception** leaves the message on the queue, so it
  becomes visible again after the visibility timeout and ultimately follows the
  **queue's own redrive policy / DLQ** (defined in the consuming service's infra,
  not in this library).

## Message translation

The raw SQS message is run through a translator before reaching the handler:

- `SqsJsonMessageTranslator` parses the JSON `Body` into `content` and puts the
  rest of the message into `metadata`.
- `AbstractMessageTranslator` is the base class for custom translators.
- A **translate-stage failure or empty content** is routed exactly like a handler
  exception (the message is not deleted).

## Concurrency, shutdown, and error routing

This is the loafer-compatibility behavior the workers rely on:

- `CancelledError` (a `BaseException`) is allowed to **propagate** so shutdown is
  clean; ordinary `Exception` is routed to the `error_handler`.
- Errors are logged (`structlog`) as `SQS Consumer: Message error` /
  `Receive error` via `log.exception`.
- On shutdown, worker tasks are **cancelled and awaited before** the shared
  aiobotocore client is closed, so in-flight work drains cleanly.

There is no Sentry integration and no `LOG_LEVEL` handling here — the host
service owns logging configuration and destination.
