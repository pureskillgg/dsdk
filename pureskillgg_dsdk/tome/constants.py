import os
import structlog


def get_tome_manifest_key_fs(path, tome_name):
    return os.path.join(path, tome_name, "tome")


def get_page_key_fs(path, subtype, page):
    name = page[subtype + "Key"]
    return os.path.join(path, name)


def filter_ds_reader_logs(_, __, event_dict):
    if event_dict.get("client") == "ds_reader_fs":
        raise structlog.DropEvent
    return event_dict
