import os
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
