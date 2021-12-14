import os


def get_tome_manifest_key_fs(path):
    return os.path.join(path, "tome")


def get_tome_path_fs(root_path, prefix, tome_name):
    tome_parent_path = root_path if prefix is None else os.path.join(root_path, prefix)
    return os.path.join(tome_parent_path, tome_name)


def get_page_key_fs(path, subtype, page):
    name = page[subtype]
    return os.path.join(path, name)
