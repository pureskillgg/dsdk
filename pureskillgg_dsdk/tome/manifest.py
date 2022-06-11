from datetime import datetime, timezone
from uuid import uuid4
import structlog
from .constants import make_key

# pylint: disable=too-many-arguments
class TomeManifest:
    def __init__(
        self,
        *,
        tome_name,
        ds_type,
        is_header=False,
        header_tome_name=None,
        src_id=None,
        is_copied_header=False,
        log=None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._tome_name = tome_name
        self._ds_type = ds_type
        self._is_copied_header = is_copied_header
        self._data = create_manifest(
            self._tome_name,
            self._ds_type,
            is_header,
            header_tome_name,
            src_id,
            is_copied_header=self._is_copied_header,
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
            "keyset": {
                "keyset": content_name(page_number, "keyset"),
                "key": make_key(
                    [
                        "tome",
                        self._ds_type,
                        self._tome_name,
                        "header" if self._is_copied_header else "",
                        content_name(page_number, "keyset"),
                    ]
                ),
                "contentType": "application/x-parquet",
            },
            "dataframe": {
                "dataframe": content_name(page_number, "dataframe"),
                "key": make_key(
                    [
                        "tome",
                        self._ds_type,
                        self._tome_name,
                        "header" if self._is_copied_header else "",
                        content_name(page_number, "dataframe"),
                    ]
                ),
                "contentType": "application/x-parquet",
            },
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


def create_manifest(
    tome_name, ds_type, is_header, header_tome_name, src_id, is_copied_header=False
):
    key = make_key(
        ["tome", ds_type, tome_name, "header" if is_copied_header else "", "tome"]
    )
    return {
        "id": str(uuid4()),
        "key": key,
        "tome": tome_name,
        "sourceId": src_id,
        "type": "tome",
        "isHeader": is_header,
        "headerTomeName": header_tome_name,
        "dsType": ds_type,
        "createdAt": now_to_iso(),
        "isComplete": False,
        "pages": [],
    }


def now():
    return datetime.now(timezone.utc)


def now_to_iso():
    return now().isoformat()
