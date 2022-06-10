import os
import structlog


def get_tome_manifest_key_fs(path, prefix, ds_type, tome_name, is_copied_header=False):
    return os.path.join(
        path,
        prefix if prefix is not None else "",
        ds_type,
        tome_name,
        "header" if is_copied_header else "",
        "tome",
    )


def get_page_key_fs(path, subtype, page):
    return os.path.join(path, page[subtype]["key"])


def filter_ds_reader_logs(_, __, event_dict):
    if event_dict.get("client") == "ds_reader_fs":
        raise structlog.DropEvent
    return event_dict
