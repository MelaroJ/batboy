import logging

import duckdb
import polars as pl
from rich.logging import RichHandler

from batboy.config.constants import INFO_DB_PATH


def setup_logger(name: str = "batboy", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = RichHandler(markup=True, show_path=False, show_time=True)
        formatter = logging.Formatter("%(message)s", datefmt="[%X]")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    logger.propagate = False
    return logger


def append_to_duckdb(df: pl.DataFrame):
    con = duckdb.connect(INFO_DB_PATH)
    con.register("new_data", df)
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {"season_info"} AS
        SELECT * FROM new_data LIMIT 0
    """)
    print(f"Appending {df.shape[0]} rows to duckdb.")
    con.sql(f"INSERT INTO {'season_info'} SELECT * FROM new_data")
    con.unregister("new_data")
    con.close()


def get_completed_org_ids() -> set[int]:
    con = duckdb.connect(INFO_DB_PATH)
    existing = {row[0] for row in con.sql("SHOW TABLES").fetchall()}
    if "season_info" not in existing:
        return set()
    org_ids = con.sql("SELECT DISTINCT org_id FROM season_info").fetchall()
    return {row[0] for row in org_ids}
