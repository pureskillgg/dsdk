# pylint: disable=missing-docstring
# pylint: disable=unused-import

import pytest
from structlog import get_logger

from .model import create_ds_models


def test_no_models():
    model = create_ds_models(models=[], log=get_logger())
    assert model is not None
