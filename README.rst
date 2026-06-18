PureSkill.gg Data Science Development Kit
=========================================

|PyPI| |GitHub Actions|

.. |PyPI| image:: https://img.shields.io/pypi/v/pureskillgg-dsdk.svg
   :target: https://pypi.python.org/pypi/pureskillgg-dsdk
   :alt: PyPI
.. |GitHub Actions| image:: https://github.com/pureskillgg/dsdk/workflows/main/badge.svg
   :target: https://github.com/pureskillgg/dsdk/actions
   :alt: GitHub Actions

Python Data Science Development Kit.

Description
-----------

``pureskillgg-dsdk`` is the generic PureSkill.gg data-science SDK: a pure-Python
library (package ``pureskillgg_dsdk``, published to PyPI) that provides the shared
building blocks the CS2 ML / analytics code is written against. It reads
parsed-demo data from S3, assembles training datasets ("tomes"), invokes ML
models, consumes SQS work queues, and exports AWS Data Exchange revisions.

This is a foundational **library, not a runtime service**. There are no Lambdas,
no serverless config, no Terraform/CDK, and no CLI entry points here — everything
is exported classes and functions consumed by other repos.

What it does
------------

The top-level ``pureskillgg_dsdk/__init__.py`` re-exports a public API spread
across five subpackages.

**ds_io** — the reader layer for parsed-demo "ds" objects (default ``ds_type``
``csds``, the parsed Counter-Strike demo data written to S3 as a JSON manifest
plus per-channel parquet). ``GameDsLoader`` wraps a reader (``DsReaderS3`` or
``DsReaderFs``) and exposes ``get_channels`` / ``get_channel``, which look up a
named channel in the object's manifest and read its parquet payload into a pandas
DataFrame; only ``application/x-parquet`` channels are supported. ``DsReaderS3``
fetches the (optionally gzip-encoded) JSON manifest and S3 object metadata, and
reads channel parquet via ``pd.read_parquet`` with a fallback path that re-reads
the raw bytes through boto3 to work around a flaky Arrow/S3
``FileNotFoundError``.

**tome** — builds and reads "tomes": page-chunked training datasets aggregated
across many matches. ``TomeCuratorFs`` is the high-level filesystem API (paths and
``ds_type`` come from ``PURESKILLGG_TOME_*`` env vars). It creates header tomes
(one row per match, scanned from a ds collection on disk), creates filtered
subheader tomes via a selector, makes data tomes by iterating a header's keyset
and concatenating per-channel DataFrames (``TomeMaker`` + ``TomeScribe`` +
``TomeManifest``, with resumable continue/overwrite/pass/fail semantics and
page-size / row-count splitting), and reads them back (``TomeLoader``,
``get_dataframe`` / ``get_keyset`` / ``iterate_pages``, ``get_match_by_index`` /
``get_random_match``). See `docs/tome-data-model.md <docs/tome-data-model.md>`_.

**ds_models** — a model-invocation registry. ``create_ds_models(...).get_ds_model(name)``
picks the first version of a named model and instantiates one of several backends:
``SagemakerEndpoint`` (CSV to a SageMaker endpoint to a JSON DataFrame),
``S3Scikit`` / ``S3ScikitSet`` (pickled scikit-learn models from S3), ``S3Dataframe``
/ ``S3DataframeSet`` (CSV/parquet lookup tables), and ``S3Hashmap`` (JSON
key-to-value). The ``*Set`` variants pick a specific artifact by matching a filter
dict.

**sqs** — ``SqsConsumer``, an asyncio / aiobotocore long-poll consumer with
bounded concurrency that is the in-house replacement (added in dsdk 3.0) for the
abandoned ``loafer`` package. It mirrors the loafer handler contract and is what
the deployed Python workers use to pull jobs off SQS. See
`docs/sqs-consumer.md <docs/sqs-consumer.md>`_.

**adx** — wraps AWS Data Exchange: list/filter dataset revisions by comment date,
and export revisions to the local filesystem or S3 (single, multiple, or
auto-export via event actions), used for publishing and retrieving the academic
data products.

Pipeline role
-------------

This package ships to PyPI as ``pureskillgg-dsdk`` (currently v3.0.1) and is
imported by the Python data-science / coaching repos — ``csgo-dsdk``,
``csgo-datascience``, ``csgo-ppp``, ``csgo-coach`` (assistant-coach), and
``csgo-progression``, plus the analysis workers.

It sits **downstream of the demo parsers**: it reads the per-match "ds" objects
(default ``ds_type`` ``csds``) that the replay / csds stage produces, and it
provides the dataset-assembly, model-invocation, and queue-consumer plumbing
those analytics jobs run on. Its ``sqs.SqsConsumer`` is the loafer replacement the
deployed workers use to consume SQS.

Public API
----------

Exported from ``pureskillgg_dsdk`` (confirmed via ``__init__.py``):

- ``create_ds_models`` — model registry factory (``ds_models``).
- ``DsReaderS3`` / ``DsReaderFs`` — backend readers for ds objects (manifest,
  metadata, channel parquet) from S3 or local filesystem (``ds_io``).
- ``GameDsLoader`` — resolves a channel in the manifest and loads its parquet
  into a DataFrame (``ds_io``).
- ``TomeCuratorFs`` / ``create_tome_curator`` — high-level make/read API for tomes
  (``tome``).
- ``SqsConsumer`` — asyncio/aiobotocore SQS long-poll consumer (``sqs``).
- ``DeleteMessage`` — exception a handler may raise to force-ack (delete) a poison
  message (``sqs``).
- ``AbstractMessageTranslator`` / ``SqsJsonMessageTranslator`` — translate a raw
  SQS message (JSON ``Body`` to content, the rest to metadata) for the consumer
  (``sqs``).

Major components
----------------

Only components/resources confirmed in the source are listed. This library owns
**no** cloud resources — every bucket, queue, dataset, and endpoint identifier is
a constructor or function argument supplied by the caller.

ds_io
~~~~~

- **GameDsLoader** — ``get_channels`` / ``get_channel`` resolve a channel in the
  ds manifest and load its parquet into a pandas DataFrame.
- **DsReaderS3 / DsReaderFs** — read the manifest (gzip-aware JSON via
  ``ContentEncoding``), metadata (S3 ``head_object``), and channel parquet, with a
  ``FileNotFoundError`` boto3 fallback and ``handle_value_error`` for parquet
  ``ValueError``. ``DsReaderS3`` takes the **bucket** as a constructor arg
  (``<passed-in>``).

tome
~~~~

- **TomeCuratorFs / create_tome_curator** — ``create_header_tome``,
  ``create_subheader_tome``, ``make_tome``, ``get_dataframe`` / ``get_keyset`` /
  ``get_manifest`` / ``iterate_pages``, ``get_match_by_index`` /
  ``get_random_match``.
- **TomeMaker / TomeScribe / TomeManifest / TomeLoader** — iterate a keyset and
  concat per-match DataFrames into page-chunked parquet with a manifest;
  resumable (continue/overwrite/pass/fail); read pages back.

ds_models
~~~~~~~~~

- **create_ds_models / DsModels.get_ds_model** — select the first version of a
  named model and build the matching backend.
- **SagemakerEndpoint** — invokes a SageMaker runtime endpoint
  (``boto3.client('runtime.sagemaker').invoke_endpoint``); ``endpoint_name`` comes
  from the model dict (``<passed-in>``).
- **S3Scikit** — pickled scikit-learn model from S3: ``MiniBatchKMeans``
  (``predict``) and ``SGDClassifier`` (``predict_proba``).
- **S3ScikitSet** — adds ``hdbscan.approximate_predict`` and requires an injected
  ``hdbscan`` module (raises ``hdbscan must be injected`` if ``None``); reach it via
  ``create_ds_models(hdbscan=...)``.
- **S3Dataframe / S3DataframeSet** — CSV/parquet lookup tables.
- **S3Hashmap** — JSON key-to-value lookups.

  Dispatch is keyed on ``model['type']`` and individual S3 backends further branch
  on ``model['model_type']`` / ``model['res_type']``; ``find_matching_model`` powers
  the ``*Set`` artifact selection by filter dict.

sqs
~~~

- **SqsConsumer** — ``consume()`` / ``start()`` / ``run()`` / ``stop()``, bounded
  concurrency. Handler contract: a truthy return (or raising ``DeleteMessage``)
  acks/deletes the message; a falsy return or exception leaves it for the queue's
  redrive policy. Takes the **queue** name or URL as the ``queue`` kwarg
  (``<passed-in>``; resolved via ``get_queue_url`` when it contains no ``://``) and an
  optional ``region_name``.
- **SqsJsonMessageTranslator / AbstractMessageTranslator** — JSON ``Body`` to
  content, the rest to metadata.
- **DeleteMessage** — force-ack exception.

adx
~~~

- **get_adx_dataset_revisions** — list/filter revisions by comment date.
- **download_adx_dataset_revision** — export a revision to the local filesystem.
- **export_single_adx_dataset_revision_to_s3** /
  **export_multiple_adx_dataset_revisions_to_s3** — create
  ``EXPORT_REVISIONS_TO_S3`` jobs (``KeyPattern`` ``<prefix>${Asset.Name}``).
- **enable_auto_exporting_adx_dataset_revisions_to_s3** /
  **disable_auto_exporting_adx_dataset_revisions_to_s3** — manage a
  ``RevisionPublished`` auto-export event action.

  All ``dataset_id`` / ``revision_id`` values and the destination **bucket** are
  ``<passed-in>``; the ``dataexchange`` client is used.

Logs and observability
----------------------

This repo is a **pure-Python library**, not a deployed service. It contains no
``serverless.yml``, no Terraform/CDK/SAM, no Lambda/Step Function/EventBridge/
AppSync/DynamoDB/SNS definitions, declares **no log groups**, and has no Sentry
integration and no ``LOG_LEVEL`` env handling. There are therefore no log groups,
DLQs, or dashboards to find *in this repo* — those belong to the consuming
service. The ``.github/workflows`` (main, format, publish, version) are CI/CD only
(uv lint/test, PyPI publish, git tag) with no AWS deploy.

How failures surface, within the host service that imports the library:

- **Logging** uses ``structlog`` (``get_logger`` / ``log.bind(...)``). Log
  destination and level are owned by the host. If the host is a Lambda or ECS
  task, look under that service's own log group (e.g. ``/aws/lambda/...``).
- **SqsConsumer** does **not** delete a message when the handler returns falsy or
  raises; errors are routed through ``error_handler`` and logged as
  ``SQS Consumer: Message error`` / ``Receive error`` (via ``log.exception``).
  Undeleted messages stay on the source queue and rely on **that queue's own
  redrive policy / DLQ**, which is defined in the consuming service's infra, not
  here. A handler may raise ``DeleteMessage`` to force-ack a poison message.
- **ADX export jobs** poll job state and raise on ``ERROR`` / ``CANCELLED`` /
  ``TIMED_OUT``.
- **TomeCuratorFs** reads ``PURESKILLGG_TOME_*`` env vars and raises
  ``Missing option ...`` if neither the arg nor the env var is set.
- **ds_io** raises on a missing manifest key and handles parquet ``ValueError``
  via ``handle_value_error``.

Configuration
-------------

``TomeCuratorFs`` reads these environment variables (each can also be passed as an
arg; ``get_env_option`` raises ``Missing option {name}, pass in or set {KEY}`` if
neither is present):

- ``PURESKILLGG_TOME_DEFAULT_HEADER_NAME``
- ``PURESKILLGG_TOME_DS_TYPE`` (default ``csds``)
- ``PURESKILLGG_TOME_COLLECTION_PATH``
- ``PURESKILLGG_TOME_DS_COLLECTION_PATH``

Documentation
-------------

- `docs/tome-data-model.md <docs/tome-data-model.md>`_ — the tome / channel
  dataset data model: how parsed match channels are stored and read, and the
  ``TomeMaker`` / ``TomeScribe`` resume/paging logic.
- `docs/sqs-consumer.md <docs/sqs-consumer.md>`_ — the ``SqsConsumer`` handler
  contract, concurrency, shutdown, and error routing (the loafer-compatibility
  behavior the workers depend on).

Installation
------------

This package is registered on the `Python Package Index (PyPI)`_
as pureskillgg-dsdk_.

Install it with

::

    $ uv add pureskillgg-dsdk

.. _pureskillgg-dsdk: https://pypi.python.org/pypi/pureskillgg-dsdk
.. _Python Package Index (PyPI): https://pypi.python.org/

Development and Testing
-----------------------

Quickstart
~~~~~~~~~~

::

    $ git clone https://github.com/pureskillgg/dsdk.git
    $ git lfs install
    $ git lfs pull
    $ cd dsdk
    $ uv sync

Run each command below in a separate terminal window:

::

    $ make watch

Primary development tasks are defined in the `Makefile`.

Source Code
~~~~~~~~~~~

The `source code`_ is hosted on GitHub.
Clone the project with

::

    $ git clone https://github.com/pureskillgg/dsdk.git
    $ git lfs install
    $ git lfs pull

.. _source code: https://github.com/pureskillgg/dsdk

Requirements
~~~~~~~~~~~~

You will need `Python 3`_ and uv_.

Install the development dependencies with

::

    $ uv sync

.. _uv: https://docs.astral.sh/uv/
.. _Python 3: https://www.python.org/

Tests
~~~~~

Lint code with

::

    $ make lint


Run tests with

::

    $ make test

Run tests on changes with

::

    $ make watch

Publishing
~~~~~~~~~~

Use the `uv version`_ command to release a new version.
Then run `make version` to commit and push a new git tag
which will trigger a GitHub action.

Publishing may be triggered using on the web
using a `workflow_dispatch on GitHub Actions`_.

.. _uv version: https://docs.astral.sh/uv/reference/cli/#uv-version
.. _workflow_dispatch on GitHub Actions: https://github.com/pureskillgg/dsdk/actions?query=workflow%3Aversion

GitHub Actions
--------------

*GitHub Actions should already be configured: this section is for reference only.*

The following repository secrets must be set on GitHub Actions.

- ``PYPI_API_TOKEN``: API token for publishing on PyPI.

These must be set manually.

Secrets for Optional GitHub Actions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The version and format GitHub actions
require a user with write access to the repository
including access to read and write packages.
Set these additional secrets to enable the action:

- ``GH_USER``: The GitHub user's username.
- ``GH_TOKEN``: A personal access token for the user.
- ``GIT_USER_NAME``: The name to set for Git commits.
- ``GIT_USER_EMAIL``: The email to set for Git commits.
- ``GPG_PRIVATE_KEY``: The `GPG private key`_.
- ``GPG_PASSPHRASE``: The GPG key passphrase.

.. _GPG private key: https://github.com/marketplace/actions/import-gpg#prerequisites

Contributing
------------

Please submit and comment on bug reports and feature requests.

To submit a patch:

1. Fork it (https://github.com/pureskillgg/dsdk/fork).
2. Create your feature branch (`git checkout -b my-new-feature`).
3. Make changes.
4. Commit your changes (`git commit -am 'Add some feature'`).
5. Push to the branch (`git push origin my-new-feature`).
6. Create a new Pull Request.

License
-------

This Python package is licensed under the MIT license.

Warranty
--------

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
