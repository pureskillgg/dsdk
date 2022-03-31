import os
import time
import urllib

import structlog
import boto3


class DataexchangeDataset:
    def __init__(self, *, root_path, prefix=None, dataset_id, log=None):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="dataexchange_dataset",
            root_path=root_path,
            prefix=prefix,
            dataset_id=dataset_id,
        )
        self._client = boto3.client("dataexchange")
        self.dataset_id = dataset_id
        self.path = get_dataset_path(root_path, prefix)
        self.dataset_name = None
        self._dataset = None

    def download_revision(self, revision_id=None):
        self._init()
        rev_id = revision_id if revision_id is not None else self._get_latest_revision()
        self._log.info(
            "Download Revision: Start", revision_id=rev_id, output_path=self.path
        )

        assets = self._get_assets(rev_id)
        for asset in assets:
            self._download_asset(asset)

        self._log.info(
            "Download Revision: End", revision_id=rev_id, output_path=self.path
        )

    def _init(self):
        if self._dataset is not None:
            return

        res = self._client.get_data_set(DataSetId=self.dataset_id)
        self.dataset_name = res.get("Name")
        self.path = os.path.join(self.path, self.dataset_name)

    def _get_latest_revision(self):
        res = self._client.list_data_set_revisions(DataSetId=self.dataset_id)
        return res.get("Revisions")[0].get("Id")

    def _get_assets(self, revision_id):
        res = self._client.list_revision_assets(
            DataSetId=self.dataset_id, RevisionId=revision_id
        )
        assets = res.get("Assets")
        while "NextToken" in res:
            res = self._client.list_revision_assets(
                DataSetId=self.dataset_id,
                RevisionId=revision_id,
                NextToken=res.get("NextToken"),
            )
            assets.extend(res.get("Assets", []))
        return assets

    def _download_asset(self, asset):
        asset_name = asset.get("Name")
        output_path = os.path.join(self.path, (os.sep).join(asset_name.split("/")))
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        if os.path.isfile(output_path):
            return

        url = self._get_download_asset_signed_url(asset)
        urllib.request.urlretrieve(url, output_path)
        self._log.info("Downloaded Asset", path=output_path, asset_name=asset_name)

    def _get_download_asset_signed_url(self, asset):
        job = self._client.create_job(
            Type="EXPORT_ASSET_TO_SIGNED_URL",
            Details={
                "ExportAssetToSignedUrl": {
                    "AssetId": asset.get("Id"),
                    "DataSetId": asset.get("DataSetId"),
                    "RevisionId": asset.get("RevisionId"),
                }
            },
        )

        job_id = job.get("Id")
        self._client.start_job(JobId=job_id)

        while True:
            time.sleep(1)
            job = self._client.get_job(JobId=job_id)

            if job.get("State") == "ERROR":
                message = job.get("Errors")[0].get("Message")
                raise Exception(f"Job {job_id} failed to complete: {message}")

            if job.get("State") == "COMPLETED":
                return job.get("Details").get("ExportAssetToSignedUrl").get("SignedUrl")


def get_dataset_path(root_path, prefix):
    parent_path = root_path if prefix is None else os.path.join(root_path, prefix)
    return parent_path
