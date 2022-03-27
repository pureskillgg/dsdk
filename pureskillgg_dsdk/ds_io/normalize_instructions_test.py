# pylint: disable=missing-docstring
# pylint: disable=unused-import

import pytest
from .normalize_instructions import normalize_instructions


def test_combining_instructions():
    instructions = normalize_instructions(
        [
            {"channel": "round_end", "columns": ["tick"]},
            {"channel": "round_end", "columns": ["win_reason_code"]},
        ]
    )
    assert instructions == [
        {"channel": "round_end", "columns": ["tick", "win_reason_code"]}
    ]


def test_combining_instructions_no_col_specified():
    instructions = normalize_instructions(
        [{"channel": "round_end", "columns": ["tick"]}, {"channel": "round_end"}]
    )
    assert instructions == [{"channel": "round_end"}]


def test_combining_three_instructions():
    instructions = normalize_instructions(
        [
            {"channel": "round_end", "columns": ["tick"]},
            {"channel": "round_end", "columns": ["win_reason_code"]},
            {"channel": "round_end", "columns": ["winner_team_code"]},
        ]
    )
    assert instructions == [
        {
            "channel": "round_end",
            "columns": ["tick", "win_reason_code", "winner_team_code"],
        }
    ]


def test_combining_several_instructions():
    instructions = normalize_instructions(
        [
            {"channel": "ch1", "columns": ["col1"]},
            {"channel": "ch1", "columns": ["col2"]},
            {"channel": "ch2", "columns": ["col1"]},
            {"channel": "ch2", "columns": ["col1", "col2"]},
            {"channel": "ch2", "columns": ["col4"]},
            {"channel": "ch2", "columns": ["col1", "col2", "col3", "col4"]},
            {"channel": "ch3", "columns": ["col1"]},
            {"channel": "ch3", "columns": ["col1", "col2"]},
            {"channel": "ch3", "columns": ["col3"]},
            {"channel": "ch3"},
        ]
    )
    assert instructions == [
        {"channel": "ch1", "columns": ["col1", "col2"]},
        {"channel": "ch2", "columns": ["col1", "col2", "col4", "col3"]},
        {"channel": "ch3"},
    ]
