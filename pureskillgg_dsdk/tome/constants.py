import os
import re
import warnings
import structlog


def get_page_path_fs(path, subtype, page):
    return os.path.join(path, page[subtype]["key"])


def filter_ds_reader_logs(_, __, event_dict):
    if event_dict.get("client") == "ds_reader_fs":
        raise structlog.DropEvent
    return event_dict


def valid_key_part(part):
    if part is None or part == "":
        return False
    return True


def make_key(my_list):
    return "/".join([arg for arg in my_list if valid_key_part(arg)])


def warn_if_invalid_tome_name(tome_name):
    if not re.match(r"\S+.\d{4}-\d{2}-\d{2},\d{4}-\d{2}-\d{2}[.\S]*", tome_name):
        warnings.warn(
            f"Header name of {tome_name} does not match convention of"
            f" tome_name.start-date,end-date[.comment] where dates "
            f"are formatted liky yyyy-mm-dd"
        )
