from datetime import datetime, timezone
from uuid import uuid4


class TomeManifest:
    def __init__(self, *, tome_name, path, ds_type):
        self._data = create_manifest(tome_name, path, ds_type)

    def add_page(self, page_number):
        page = {
            "number": page_number,
            "keyset": content_name(page_number, "keyset"),
            "dataframe": content_name(page_number, "dataframe"),
            "keysetContentType": "application/x-parquet",
            "dataframeContentType": "application/x-parquet",
        }
        self._data["pages"].append(page)
        return page

    def get(self):
        return self._data


def content_name(page_number, subtype):
    return f"{subtype}_{str(page_number).zfill(5)}"


def create_manifest(tome_name, path, ds_type):
    return {
        "id": str(uuid4()),
        "type": "tome",
        "ds_type": ds_type,
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "tome": tome_name,
        "path": path,
        "pages": [],
    }
