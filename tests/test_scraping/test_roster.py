# tests/test_scraping/test_roster.py

import polars as pl

from batboy.scraping.roster import scrape_roster

SEASON_ID = 596721  # Tennessee 2024–25


def test_scrape_roster_basic():
    df = scrape_roster(SEASON_ID)

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0

    expected_columns = {
        "season_id",
        "player_id",
        "player_url",
        "name",
        "gp",
        "gs",
        "number",
        "class",
        "position",
        "height",
        "bats",
        "throws",
        "hometown",
        "highschool",
    }
    assert expected_columns.issubset(set(df.columns))

    assert df["player_id"].drop_nulls().len() > 0
    assert df["name"].drop_nulls().len() > 0
    assert df["player_url"].drop_nulls().len() > 0


def test_scrape_roster_sorted_by_name():
    df = scrape_roster(SEASON_ID, sort_by="name")
    assert df.shape[0] > 1
    names = df["name"].to_list()
    assert names == sorted(names)
