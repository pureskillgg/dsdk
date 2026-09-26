# pylint: disable=missing-docstring

import pytest

from .find import find_matching_model


def create_models():
    return [
        {"key": "dust2_a", "map_name": "de_dust2"},
        {"key": "dust2_b", "map_name": "de_dust2"},
        {"key": "mirage", "map_name": "de_mirage"},
    ]


def test_find_matching_model():
    models = create_models()

    assert find_matching_model(models, {"map_name": "de_mirage"}) == models[2]
    assert find_matching_model(models, {"map_name": "de_nuke"}) is None
    assert [model["key"] for model in models] == ["dust2_a", "dust2_b", "mirage"]


def test_find_matching_model_reports_match_count():
    models = create_models()

    with pytest.raises(Exception, match="Found 2 matches"):
        find_matching_model(models, {"map_name": "de_dust2"})
    assert [model["key"] for model in models] == ["dust2_a", "dust2_b", "mirage"]
