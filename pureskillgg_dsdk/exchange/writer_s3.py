import time

import structlog


class AdxDatasetWriterS3:
    def __init__(self, *, bucket, prefix=None, log=None):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(client="adx_dataset_writer_s3")
        self._bucket = bucket
        self._prefix = prefix

    def export_revision(self, client, dataset_id, revision_id):
        res = client.create_job(
            Details={
                "ExportRevisionsToS3": {
                    "DataSetId": dataset_id,
                    "RevisionDestinations": [
                        {
                            "Bucket": self._bucket,
                            "KeyPattern": self._get_key_pattern(),
                            "RevisionId": revision_id,
                        },
                    ],
                }
            }
        )

        job_id = res["id"]

        client.start_job(job_id)

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
                return

    def auto_export_revisions(self, client, dataset_id):
        client.create_event_action(
            Action={
                "ExportRevisionToS3": {
                    "RevisionDestination": {
                        "Bucket": self._bucket,
                        "KeyPattern": self._get_key_pattern(),
                    }
                }
            },
            Event={"RevisionPublished": {"DataSetId": dataset_id}},
        )

    def _get_key_pattern(self):
        pattern_prefix = self._prefix if self._prefix is not None else ""
        return ("".join([pattern_prefix, "${Asset.Name}"]),)