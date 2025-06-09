import polars as pl

from batboy.scraping.teams import get_ncaa_baseball_teams


def test_get_ncaa_baseball_teams_returns_valid_dataframe():
    df = get_ncaa_baseball_teams()
    assert isinstance(df, pl.DataFrame)
    assert "org_id" in df.columns
    assert "school_name" in df.columns
    assert df.shape[0] > 0
    assert df["org_id"].dtype == pl.Int64
    assert df["school_name"].dtype == pl.String
