import polars as pl

from batboy.scraping.teams import get_team_seasons_from_rows, scrape_team_history_table


def test_scrape_team_history_table_returns_list():
    org_id = 694  # Tennessee
    rows = scrape_team_history_table(org_id)
    assert isinstance(rows, list)
    assert len(rows) > 0
    assert isinstance(rows[0], dict)
    assert "season_id" in rows[0]
    assert "coach" in rows[0]


def test_get_team_seasons_from_rows_returns_polars_df():
    dummy_rows = [
        {
            "org_id": 694,
            "season_id": 123456,
            "year": "2024-25",
            "coach": "Tony Vitello",
            "division": "D-I",
            "conference": "SEC",
            "wins": 46,
            "losses": 19,
            "ties": 0,
            "win_pct": 0.708,
            "notes": "",
        }
    ]
    df = get_team_seasons_from_rows(dummy_rows)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 1
    assert set(df.columns) >= {"season_id", "year", "wins"}
