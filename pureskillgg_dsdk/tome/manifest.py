from datetime import datetime, timezone
from uuid import uuid4
import structlog

# pylint: disable=too-many-arguments
class TomeManifest:
    def __init__(
        self,
        *,
        tome_name,
        path,
        ds_type,
        is_header=False,
        header_tome_name=None,
        src_id=None,
        log=None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._data = create_manifest(
            tome_name, path, ds_type, is_header, header_tome_name, src_id
        )
        self._current_page_start_time = None
        self._current_page_end_time = None

    def set(self, data):
        self._data = data

    def start_page(self):
        self._current_page_start_time = now()

    def end_page(self, page_number):
        self._current_page_end_time = now()
        page = {
            "number": page_number,
            "keyset": content_name(page_number, "keyset"),
            "dataframe": content_name(page_number, "dataframe"),
            "keysetContentType": "application/x-parquet",
            "dataframeContentType": "application/x-parquet",
            "createdAt": now_to_iso(),
            "timings": self._calculate_end_page_timings(),
        }
        self._data["pages"].append(page)
        return page

    def finish(self):
        self._data["isComplete"] = True
        self._data["timings"] = self._sum_page_timings()
        self._data["completedAt"] = now_to_iso()

    def get(self):
        return self._data

    def _sum_page_timings(self):
        timings = {
            "seconds": 0,
            "minutes": 0,
            "hours": 0,
            "days": 0,
        }
        for page in self._data["pages"]:
            for key in timings:
                timings[key] += page["timings"][key]
        return timings

    def _calculate_end_page_timings(self):
        time_elapsed = (
            self._current_page_end_time.timestamp()
            - self._current_page_start_time.timestamp()
        )
        return {
            "seconds": time_elapsed,
            "minutes": time_elapsed / 60,
            "hours": time_elapsed / 60 / 60,
            "days": time_elapsed / 60 / 60 / 24,
        }


def content_name(page_number, subtype):
    return f"{subtype}_{str(page_number).zfill(5)}"


def create_manifest(tome_name, path, ds_type, is_header, header_tome_name, src_id):
    return {
        "id": str(uuid4()),
        "sourceId": src_id,
        "type": "tome",
        "isHeader": is_header,
        "headerTomeName": header_tome_name,
        "dsType": ds_type,
        "createdAt": now_to_iso(),
        "tome": tome_name,
        "path": path,
        "isComplete": False,
        "pages": [],
    }


def now():
    return datetime.now(timezone.utc)


def now_to_iso():
    return now().isoformat()
