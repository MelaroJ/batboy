# tests/test_scraping/test_game_logs.py

import polars as pl

from batboy.config.constants import (
    FLOAT32_COLUMNS,
    INT64_COLUMNS,
    STAT_CATEGORY_SCHEMAS,
)
from batboy.scraping.game_logs import get_team_game_log_player_ids, scrape_game_log


def test_get_team_game_log_player_ids_returns_expected_structure():
    season_id = 596721  # 2024-2025 Tennessee Volunteers

    player_id_map = get_team_game_log_player_ids(season_id)

    assert isinstance(player_id_map, dict)
    assert all(isinstance(k, int) for k in player_id_map)
    assert all(isinstance(v, str) for v in player_id_map.values())
    assert len(player_id_map) > 5  # should have team + players

    # Check that team stats player_id is in the result
    # (Team = first <option>, ID 8903722 as of June 2025)
    team_player_id = 8903722
    assert team_player_id in player_id_map
    assert "Tennessee stats" in player_id_map[team_player_id]


def test_scrape_game_log_typing_and_structure():
    player_id = 8903722  # 2024-25 Tennessee Team Totals
    stat_type = "hitting"

    df = scrape_game_log(player_id, stat_type)

    # Check output is a DataFrame
    assert isinstance(df, pl.DataFrame)

    # Check that schema includes expected columns
    expected = set(
        STAT_CATEGORY_SCHEMAS[stat_type] + ["opponent_id", "box_score_url", "game_id"]
    )
    actual = set(df.columns)
    assert expected.issubset(actual)

    # Check that we have some data rows
    assert df.shape[0] > 0

    # Check types for known numeric columns
    for col in df.columns:
        if col in INT64_COLUMNS:
            assert df.schema[col] in (pl.Int64, pl.Int32, pl.UInt32)
        if col in FLOAT32_COLUMNS:
            assert df.schema[col] in (pl.Float32, pl.Float64)
