# batboy/data/__init__.py

from pathlib import Path

import polars as pl


def load_schools() -> pl.DataFrame:
    path = Path(__file__).parent / "ncaa_schools.parquet"
    return pl.read_parquet(path)
