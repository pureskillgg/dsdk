from .curator import TomeCuratorFs


def create_tome_curator(**kwargs):
    return TomeCuratorFs(**kwargs)
