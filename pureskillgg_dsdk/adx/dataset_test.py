# pylint: disable=missing-docstring,invalid-name,protected-access

import pytest

from .dataset import AdxDataset, is_date_between


class StubDataExchange:
    """Answers from memory; nothing here reaches AWS Data Exchange."""

    def __init__(self):
        self.get_data_set_calls = 0

    def get_data_set(self, DataSetId):
        self.get_data_set_calls += 1
        return {"Id": DataSetId, "Name": "Test dataset"}

    def list_data_set_revisions(self, DataSetId):
        assert DataSetId == "test-dataset"
        return {"Revisions": [{"Id": "rev-2"}, {"Id": "rev-1"}]}

    def get_revision(self, DataSetId, RevisionId):
        assert DataSetId == "test-dataset"
        return {"Id": RevisionId, "Comment": "2022-01-01T00:00:00Z"}


class RecordingLog:
    def __init__(self):
        self.errors = []

    def bind(self, **_kwargs):
        return self

    def info(self, *_args, **_kwargs):
        pass

    def error(self, event, **kwargs):
        self.errors.append((event, kwargs))


class FailingWriter:
    def export_revision(self, client, dataset_id, revision_id):
        raise RuntimeError(f"export of {revision_id} failed")


def create_dataset(*, writer=None, log=None):
    dataset = AdxDataset(dataset_id="test-dataset", writer=writer, log=log)
    dataset._client = StubDataExchange()
    return dataset


def test_dataset_is_fetched_once():
    dataset = create_dataset()

    assert dataset.get_latest_revision() == {"Id": "rev-2"}
    assert dataset.get_latest_revision() == {"Id": "rev-2"}
    assert dataset._client.get_data_set_calls == 1
    assert dataset.dataset_name == "Test dataset"


REVISIONS_WITH_REVOKED = [
    {"Id": "rev-4", "Comment": "2026-10-09T00:00:00.000Z", "Revoked": True},
    {"Id": "rev-3", "Comment": "2026-10-08T00:00:00.000Z"},
    {"Id": "rev-2", "Comment": "2026-10-07T00:00:00.000Z", "Revoked": False},
    {"Id": "rev-1", "Comment": "2025-07-18T00:00:00.000Z", "Revoked": True},
]


class StubPaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, DataSetId):
        assert DataSetId == "test-dataset"
        return iter(self._pages)


class StubDataExchangeWithRevoked(StubDataExchange):
    def list_data_set_revisions(self, DataSetId):
        assert DataSetId == "test-dataset"
        return {"Revisions": REVISIONS_WITH_REVOKED}

    def get_paginator(self, operation):
        assert operation == "list_data_set_revisions"
        return StubPaginator(
            [
                {"Revisions": REVISIONS_WITH_REVOKED[:2]},
                {"Revisions": REVISIONS_WITH_REVOKED[2:]},
            ]
        )


def test_revoked_revisions_are_skipped():
    dataset = AdxDataset(dataset_id="test-dataset", writer=None)
    dataset._client = StubDataExchangeWithRevoked()

    assert dataset.get_latest_revision()["Id"] == "rev-3"
    assert [rev["Id"] for rev in dataset.get_revisions()] == ["rev-3", "rev-2"]
    assert [rev["Id"] for rev in dataset.get_revisions("2025-01-01", "2026-10-08")] == [
        "rev-2"
    ]


def test_export_revision_failure_logs_exc_info():
    log = RecordingLog()
    dataset = create_dataset(writer=FailingWriter(), log=log)

    with pytest.raises(RuntimeError, match="export of rev-1 failed"):
        dataset.export_revision("rev-1")

    assert len(log.errors) == 1
    event, kwargs = log.errors[0]
    assert event == "Export Revision: Fail"
    assert isinstance(kwargs.get("exc_info"), RuntimeError)


@pytest.mark.parametrize(
    "start_date,end_date,expected",
    [
        (None, None, True),
        ("2022-01-01", "2022-02-01", True),
        ("2022-01-02", None, False),
        (None, "2022-01-01", False),
    ],
)
def test_is_date_between(start_date, end_date, expected):
    assert is_date_between("2022-01-01T12:00:00Z", start_date, end_date) is expected
