import re
from typing import Optional

import polars as pl
from selectolax.parser import HTMLParser

from batboy.config.constants import BASE_DOMAIN
from batboy.scraping.core import get_dom
from batboy.utils import setup_logger

logger = setup_logger()


def get_team_roster(season_id: int) -> pl.DataFrame:
    """
    Scrape the team roster table for a given season.

    Args:
        season_id: The numeric season ID for the team (from season_url)

    Returns:
        Polars DataFrame with normalized roster fields.
    """
    url = f"{BASE_DOMAIN}/teams/{season_id}/roster"
    logger.info(f"🔗 Fetching roster from {url}")

    dom: Optional[HTMLParser] = get_dom(url)
    if dom is None or dom.root is None:
        raise ValueError(f"❌ Failed to load DOM for roster page: {url}")

    logger.info("✅ DOM successfully retrieved.")

    # Try to locate the scrolling roster table
    scroll_body = dom.css_first(".dataTables_scrollBody")
    if not scroll_body:
        logger.warning("❌ dataTables_scrollBody not found in DOM.")
        return pl.DataFrame()

    table = scroll_body.css_first("table")
    if not table:
        logger.warning("❌ No <table> found inside dataTables_scrollBody.")
        return pl.DataFrame()

    thead = table.css_first("thead")
    tbody = table.css_first("tbody")

    if not thead or not tbody:
        logger.warning("❌ Table is missing <thead> or <tbody>.")
        return pl.DataFrame()

    # Log the header columns so we can inspect them
    headers = [th.text(strip=True) for th in thead.css("th")]
    logger.info(f"📋 Table headers: {headers}")

    rows = tbody.css("tr")
    logger.info(f"🧍 Found {len(rows)} roster rows")

    records = []

    for row in rows:
        cells = row.css("td")
        if len(cells) != len(headers):
            logger.warning(
                f"⚠️ Skipping row with {len(cells)} cells (expected {len(headers)})"
            )
            continue

        record = {}
        for i, header in enumerate(headers):
            text = cells[i].text(strip=True)
            record[header] = text

        # Extract player_id and URL from the "Name" column
        name_cell = cells[3]
        link = name_cell.css_first("a")
        player_id = None
        player_url = None
        if link:
            href = link.attrs.get("href")
            if href and "/players/" in href:
                player_url = f"{BASE_DOMAIN}{href}"
                match = re.search(r"/players/(\d+)", href)
                if match:
                    player_id = int(match.group(1))
        record["player_id"] = player_id
        record["player_url"] = player_url

        records.append(record)

    df = pl.DataFrame(records)

    # Rename columns to normalized names if they exist
    column_renames = {
        "GP": "gp",
        "GS": "gs",
        "#": "number",
        "Name": "player_name",
        "Class": "class",
        "Position": "position",
        "Height": "height",
        "Bats": "bats",
        "Throws": "throws",
        "Hometown": "hometown",
        "High School": "highschool",
    }

    # Only rename columns that exist in this dataframe
    rename_subset = {k: v for k, v in column_renames.items() if k in df.columns}
    df = df.rename(rename_subset)

    # Final column ordering
    full_schema = [
        "player_id",
        "player_name",
        "player_url",
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
    ]

    for col in full_schema:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    return df.select(full_schema)


def append_roster_data(
    df: pl.DataFrame,
    org_id: int,
    school_name: str,
    season_url: str,
    year: str,
) -> None:
    """
    Append roster data to DuckDB with team-season context.
    """
    if df.is_empty():
        return

    df = df.with_columns(
        org_id=pl.lit(org_id, dtype=pl.Int32),
        school_name=pl.lit(school_name, dtype=pl.String),
        season_url=pl.lit(season_url, dtype=pl.String),
        year=pl.lit(year, dtype=pl.String),
    )

    import duckdb

    from batboy.config.constants import ROSTER_DATA_TABLE, ROSTER_DB_PATH

    con = duckdb.connect(ROSTER_DB_PATH)
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {ROSTER_DATA_TABLE} AS
        SELECT * FROM df LIMIT 0
    """)
    con.register("df", df)
    con.sql(f"INSERT INTO {ROSTER_DATA_TABLE} SELECT * FROM df")
    con.unregister("df")
    con.close()


def log_roster_scrape(
    org_id: int,
    school_name: str,
    season_url: str,
    success: bool,
    n_players: int,
    error: Optional[str] = None,
) -> None:
    """
    Append one row to the log table to record a roster scrape attempt.
    """
    import duckdb

    from batboy.config.constants import ROSTER_DB_PATH, ROSTER_LOG_TABLE

    con = duckdb.connect(ROSTER_DB_PATH)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {ROSTER_LOG_TABLE} (
            org_id INTEGER,
            school_name TEXT,
            season_url TEXT,
            success BOOLEAN,
            n_players INTEGER,
            error TEXT
        );
    """)
    con.execute(
        f"""
        INSERT INTO {ROSTER_LOG_TABLE} (
            org_id, school_name, season_url, success, n_players, error
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (org_id, school_name, season_url, success, n_players, error),
    )
    con.close()


def get_pending_roster_targets(limit: Optional[int] = None) -> pl.DataFrame:
    """
    Return all team-seasons that have a roster tab and have not been scraped.
    """
    import duckdb

    from batboy.config.constants import INFO_DB_PATH, ROSTER_DB_PATH, ROSTER_LOG_TABLE

    con = duckdb.connect(ROSTER_DB_PATH)
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {ROSTER_LOG_TABLE} (
            org_id INTEGER,
            school_name TEXT,
            season_url TEXT,
            success BOOLEAN,
            n_players INTEGER,
            error TEXT
        );
    """)
    scraped_urls = con.sql(
        f"SELECT DISTINCT season_url FROM {ROSTER_LOG_TABLE}"
    ).fetchall()
    already_done = {row[0] for row in scraped_urls}
    con.close()

    con_info = duckdb.connect(INFO_DB_PATH)
    df = con_info.sql("""
        SELECT org_id, school_name, season_url, year
        FROM season_info
        WHERE has_roster = TRUE
    """).pl()
    con_info.close()

    pending = df.filter(~pl.col("season_url").is_in(already_done))
    if limit:
        pending = pending.head(limit)

    return pending


def batch_scrape_team_rosters(limit: Optional[int] = None) -> None:
    """
    Batch scrape team rosters for all team-seasons with has_roster = TRUE.
    Uses resume logic based on prior logs.
    """
    from batboy.scraping.rosters import (
        get_team_roster,
    )  # or inline depending on location

    logger.info(f"\n🚦 Starting batch scrape of team rosters (limit={limit})")
    pending = get_pending_roster_targets(limit)

    if pending.is_empty():
        logger.info("📭 Nothing to scrape — all team rosters are logged.")
        return

    for row in pending.iter_rows(named=True):
        org_id = row["org_id"]
        school_name = row["school_name"]
        season_url = row["season_url"]
        year = row["year"]

        logger.info(f"\n🧍 {school_name} {year} — {season_url}")

        try:
            season_id = int(season_url.strip("/").split("/")[-1])
            df = get_team_roster(season_id)
            n_players = df.shape[0]
            append_roster_data(df, org_id, school_name, season_url, year)
            log_roster_scrape(
                org_id=org_id,
                school_name=school_name,
                season_url=season_url,
                success=True,
                n_players=n_players,
            )
            logger.info(f"✅ Scraped {n_players} players.")

        except Exception as e:
            logger.error(f"❌ Failed: {e}")
            log_roster_scrape(
                org_id=org_id,
                school_name=school_name,
                season_url=season_url,
                success=False,
                n_players=0,
                error=str(e),
            )


if __name__ == "__main__":
    batch_scrape_team_rosters()
