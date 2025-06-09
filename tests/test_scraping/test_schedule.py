# tests/scraping/test_schedule.py

import polars as pl

from batboy.scraping.schedule import scrape_schedule


def test_scrape_schedule_structure():
    season_id = 596721  # Valid known season, Tennessee
    df = scrape_schedule(season_id)

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0, "No games were scraped"

    expected_columns = {
        "season_id",
        "game_id",
        "date",
        "opponent_name",
        "opponent_id",
        "result",
        "team_score",
        "opp_score",
        "attendance",
        "box_score_url",
    }
    assert set(df.columns) == expected_columns
