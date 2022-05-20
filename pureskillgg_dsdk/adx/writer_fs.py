import os
import time
import urllib

import structlog


class AdxDatasetWriterFs:
    def __init__(self, *, root_path, prefix, log=None):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="adx_dataset_writer_fs", root_path=root_path, prefix=prefix
        )
        self._path = root_path
        self._prefix = prefix

    def export_revision(self, client, dataset_id, revision_id):
        assets = get_assets(client, dataset_id, revision_id)
        if len(assets) > 500:
            self._log.warn(
                f"This revision contains {len(assets)} assets and will take a long time to download. Seriously, you are not gonna want to wait for this. Export to S3 by using export_multiple_adx_dataset_revisions_to_s3 instead."
            )
        for asset in assets:
            self._download_asset(client, asset)

    def auto_export_revisions(self, client, dataset_id):
        raise NotImplementedError("Cannot auto export revisions to file system.")

    def _download_asset(self, client, asset):
        asset_name = asset.get("Name")
        output_path = os.path.join(self._get_output_path(), *asset_name.split("/"))
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        if os.path.isfile(output_path):
            return

        url = get_download_asset_signed_url(client, asset)
        urllib.request.urlretrieve(url, output_path)
        self._log.info("Downloaded Asset", path=output_path, asset_name=asset_name)

    def _get_output_path(self):
        if self._prefix is None:
            return self._path
        return os.path.join(self._path, self._prefix)


def get_assets(client, dataset_id, revision_id):
    paginator = client.get_paginator("list_revision_assets")
    pages = paginator.paginate(DataSetId=dataset_id, RevisionId=revision_id)
    assets = []
    for page in pages:
        assets.extend(page["Assets"])
    return assets


def get_download_asset_signed_url(client, asset):
    job = client.create_job(
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
    client.start_job(JobId=job_id)

    while True:
        time.sleep(2)
        job = client.get_job(JobId=job_id)

        if job.get("State") == "ERROR":
            message = job.get("Errors")[0].get("Message")
            raise Exception(f"Job {job_id} failed: {message}")

        if job.get("State") == "CANCELLED":
            raise Exception(f"Job {job_id} was cancelled.")

        if job.get("State") == "TIMED_OUT":
            raise Exception(f"Job {job_id} timed out.")

        if job.get("State") == "COMPLETED":
            return job.get("Details").get("ExportAssetToSignedUrl").get("SignedUrl")
