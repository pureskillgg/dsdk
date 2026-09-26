PureSkill.gg Data Science Development Kit
=========================================

|PyPI| |GitHub Actions|

.. |PyPI| image:: https://img.shields.io/pypi/v/pureskillgg-dsdk.svg
   :target: https://pypi.python.org/pypi/pureskillgg-dsdk
   :alt: PyPI
.. |GitHub Actions| image:: https://github.com/pureskillgg/dsdk/workflows/main/badge.svg
   :target: https://github.com/pureskillgg/dsdk/actions
   :alt: GitHub Actions

The shared Python toolkit behind PureSkill.gg's Counter-Strike data pipeline.
It reads parsed matches (CSDS: a JSON manifest plus one parquet file per
channel) from S3 or disk into pandas, consumes SQS work queues, loads models
from S3 or SageMaker, builds multi-match datasets ("tomes"), and exports AWS
Data Exchange revisions. It is a library only: every bucket, queue and dataset
is an argument you pass in.

Installation
------------

Requires Python 3.11 or later.

::

    $ uv add pureskillgg-dsdk

The ``s3_xgboost`` model type needs the ``xgboost`` extra::

    $ uv add "pureskillgg-dsdk[xgboost]"

Usage
-----

Read CSDS channels into DataFrames:

.. code-block:: python

    from pureskillgg_dsdk import DsReaderS3, GameDsLoader

    reader = DsReaderS3(bucket="my-csds-bucket", manifest_key="path/to/match/csds")
    loader = GameDsLoader(reader=reader)

    deaths = loader.get_channel(
        {"channel": "player_death", "columns": ["round", "tick", "attacker_id"]}
    )
    data = loader.get_channels([{"channel": "header"}, {"channel": "round_end"}])

``DsReaderFs(root_path=..., manifest_key=...)`` reads the same layout from
disk. Leave out ``columns`` to read every column.

Consume an SQS queue:

.. code-block:: python

    import structlog
    from pureskillgg_dsdk import SqsConsumer

    async def handler(content, metadata):
        # content is the message's parsed JSON body.
        return True  # Delete the message. False leaves it for the redrive policy.

    consumer = SqsConsumer(
        queue="my-queue", handler=handler, log=structlog.get_logger(), concurrency=4
    )
    consumer.run()  # Blocks. consumer.start() schedules it on a running loop.

Modules
-------

.. list-table::
   :header-rows: 1

   * - Module
     - Main exports
     - Used by
   * - ``ds_io``
     - ``GameDsLoader``, ``DsReaderS3``, ``DsReaderFs``
     - csgo-coach, csgo-ppp, igl-pipeline's ``coach_evaluate`` runner
   * - ``sqs``
     - ``SqsConsumer``, ``DeleteMessage``, ``SqsJsonMessageTranslator``
     - csgo-coach, csgo-ppp
   * - ``ds_models``
     - ``create_ds_models``: SageMaker endpoints, and scikit-learn, XGBoost,
       DataFrame and hashmap models stored on S3
     - csgo-coach
   * - ``tome``
     - ``TomeCuratorFs``, ``create_tome_curator``
     - the makenew-pyskill template's notebooks
   * - ``adx``
     - ``get_adx_dataset_revisions``, ``download_adx_dataset_revision``, and
       the ``export_*`` and ``*_auto_exporting_*`` functions
     - the makenew-pyskill template's notebooks

``pureskillgg-csgo-dsdk`` uses this package in its tests only.

Documentation
-------------

- `docs/sqs-consumer.md <docs/sqs-consumer.md>`_: the ``SqsConsumer`` handler
  contract, concurrency, shutdown and error routing.
- `docs/tome-data-model.md <docs/tome-data-model.md>`_: how tomes are laid out,
  paged and resumed.

Development
-----------

You need Python 3, uv_ and `Git LFS`_ (the test fixtures are in LFS).

::

    $ git clone https://github.com/pureskillgg/dsdk.git
    $ cd dsdk
    $ git lfs install
    $ git lfs pull
    $ uv sync

The tasks are in the ``Makefile``: ``make lint``, ``make test``, ``make watch``
(tests on every change) and ``make format``.

.. _uv: https://docs.astral.sh/uv/
.. _Git LFS: https://git-lfs.com/

Publishing
~~~~~~~~~~

Set the new version with ``uv version``, then run ``make version``. It commits
``pyproject.toml`` and ``uv.lock`` and pushes a signed ``v*`` tag, which
triggers the publish workflow. Or run the `version workflow`_ by hand with a
version number or a bump (``patch``, ``minor``, ``major``); it does both steps.

Publishing needs the ``PYPI_API_TOKEN`` repository secret. The version and
format workflows also need ``GH_USER``, ``GH_TOKEN``, ``GIT_USER_NAME``,
``GIT_USER_EMAIL``, ``GPG_PRIVATE_KEY`` and ``GPG_PASSPHRASE``.

.. _version workflow: https://github.com/pureskillgg/dsdk/actions/workflows/version.yml

License
-------

MIT. See ``LICENSE.txt``.
