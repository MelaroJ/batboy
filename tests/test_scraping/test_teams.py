from typing import cast

import polars as pl
import pytest

from batboy.scraping.teams import get_ncaa_baseball_teams, get_team_seasons

# These constants are known to exist — use a stable Division I team for reliability
EXAMPLE_TEAM_NAME = "Tennessee"
EXAMPLE_TEAM_ID = 694


@pytest.mark.no_web
def test_get_ncaa_baseball_teams_returns_valid_dataframe():
    df = get_ncaa_baseball_teams()
    assert isinstance(df, pl.DataFrame)
    assert "org_id" in df.columns
    assert "school_name" in df.columns
    assert df.shape[0] > 0
    assert df["org_id"].dtype == pl.Int64
    assert df["school_name"].dtype == pl.String


@pytest.mark.web
@pytest.mark.parametrize("team", [EXAMPLE_TEAM_NAME, EXAMPLE_TEAM_ID])
def test_get_team_seasons_returns_valid_dataframe(team):
    df = get_team_seasons(team)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0

    expected_columns = {
        "org_id",
        "season_id",
        "season_url",
        "year",
        "coach",
        "division",
        "conference",
        "wins",
        "losses",
        "ties",
        "win_pct",
        "notes",
    }
    assert set(df.columns) == expected_columns

    # Basic value checks
    assert df["org_id"].unique().to_list() == [EXAMPLE_TEAM_ID]

    wins_min = df["wins"].min()
    assert wins_min is not None
    wins_min = cast(int, wins_min)
    assert wins_min >= 0

    win_pct_max = df["win_pct"].max()
    assert win_pct_max is not None
    win_pct_max = cast(float, win_pct_max)
    assert win_pct_max <= 1.0


@pytest.mark.no_web
def test_get_team_seasons_fails_on_invalid_team():
    with pytest.raises(ValueError, match="not found or ambiguous"):
        get_team_seasons("Definitely Not A School")
