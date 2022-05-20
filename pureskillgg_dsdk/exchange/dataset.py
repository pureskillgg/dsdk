import dateutil

import structlog
import boto3


class AdxDataset:
    def __init__(self, *, dataset_id, writer, log=None):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="adx_dataset",
            dataset_id=dataset_id,
        )
        self._client = boto3.client("dataexchange")
        self.dataset_id = dataset_id
        self.dataset_name = None
        self._writer = writer
        self._dataset = None

    def get_latest_revision(self):
        self._init()
        res = self._client.list_data_set_revisions(DataSetId=self.dataset_id)
        revisions = res.get("Revisions", [])
        if len(revisions) == 0:
            return None
        return revisions[0]

    def get_revisions(self, start_date=None, end_date=None, /):
        self._init()
        paginator = self._client.get_paginator("list_data_set_revisions")
        pages = paginator.paginate(DataSetId=self.dataset_id)
        revisions = []
        for page in pages:
            revisions.extend(page["Revisions"])
        return [
            rev
            for rev in revisions
            if is_date_between(rev["Comment"], start_date, end_date)
        ]

    def export_revision(self, revision_id, comment=None):
        self._init()
        log = self._log.bind(revision_id=revision_id, comment=comment)
        try:
            log("Export Revision: Start")
            self._writer.export_revision(self._client, self.dataset_id, revision_id)
            log("Export Revision: Success")
        except BaseException as err:
            log.error("Export Revision: Fail", exec_info=err)
            raise

    def auto_export_revisions(self):
        self._init()
        self._writer.auto_export_revisions(self._client, self.dataset_id)

    def delete_all_auto_revision_exports(self):
        self._init()
        raise NotImplementedError(
            "To delete an auto-export job, select the job you want to delete. Select the Actions menu in the auto-export job destinations section and choose Remove auto-export job destination."
        )

    def _init(self):
        if self._dataset is not None:
            return

        res = self._client.get_data_set(DataSetId=self.dataset_id)
        self.dataset_name = res.get("Name")
        self._log = self._log.bind(dataset_name=self.dataset_name)


def is_date_between(date, start_date, end_date):
    start = dateutil.parser.isoparse(start_date if start_date is not None else "1900")
    end = dateutil.parser.isoparse(end_date if end_date is not None else "4000")
    try:
        middle = dateutil.parser.isoparse(date)
        return start <= middle < end
    except dateutil.parser.ParserError:
        return False
