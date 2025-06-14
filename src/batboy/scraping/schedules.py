import re
from typing import Optional

import duckdb
import polars as pl
from selectolax.parser import HTMLParser

from batboy.config.constants import (
    BASE_DOMAIN,
    INFO_DB_PATH,
    SCHEDULE_DATA_TABLE,
    SCHEDULE_LOG_TABLE,
    SEASON_SCHEDULE_DB,
)
from batboy.scraping.core import get_dom
from batboy.utils import setup_logger

logger = setup_logger()


def _parse_schedule_dom(dom: HTMLParser, season_url: str) -> pl.DataFrame:
    """Extract schedule data from a pre-parsed DOM."""
    header = next(
        (
            h
            for h in dom.css(".card-header")
            if h.text(strip=True) == "Schedule/Results"
        ),
        None,
    )

    if header and header.parent:
        table = header.parent.css_first("table")
    else:
        table = dom.css_first("table.mytable")

    if table is None:
        logger.warning(f"❌ Schedule table not found at {season_url}")
        return pl.DataFrame()
    if table is None:
        logger.warning(f"❌ Schedule table not found at {season_url}")
        return pl.DataFrame()

    rows = table.css("tr")

    # Try to find the first row with at least 3 <td> cells
    data_row = next((r for r in rows if len(r.css("td")) >= 3), None)
    if not data_row:
        logger.warning(f"❌ No data rows with >=3 <td> cells at {season_url}")
        return pl.DataFrame()

    num_cols = len(data_row.css("td"))

    if num_cols == 3:
        logger.info("🕰 Detected pre-2018 schedule legacy format (no attendance column)")
        records = []
        for row in rows:
            cells = row.css("td")
            if len(cells) != 3:
                continue  # only process rows with exactly 3 columns

            date = cells[0].text(strip=True)

            # Opponent info
            opp = cells[1]
            opponent_raw = opp.text(strip=True)
            opponent_name = opponent_raw
            opponent_id = None
            opponent_site = ""
            opponent_note = ""

            a_tag = opp.css_first("a")
            if a_tag:
                href = a_tag.attrs.get("href") or ""
                m = re.search(r"/teams/(\d+)", href)
                if m:
                    opponent_id = int(m.group(1))

                anchor_text = a_tag.text(strip=True)
                opponent_name = anchor_text.strip()

                # Check for @ or vs in full opponent text
                prefix_match = re.match(r"(@|vs)?\s*(.*)", opponent_raw)
                if prefix_match:
                    if prefix_match.group(1):
                        opponent_site = prefix_match.group(1)
                    remainder = prefix_match.group(2)
                    if opponent_name in remainder:
                        opponent_note = remainder.replace(opponent_name, "").strip()

            # Result and game_id
            result_cell = cells[2]
            result = result_cell.text(strip=True)
            team_score = opp_score = innings = game_id = None

            m = re.search(r"[WL] (\d+)\s*-\s*(\d+)(?: \((\d+)\))?", result)
            if m:
                team_score = int(m.group(1))
                opp_score = int(m.group(2))
                if m.group(3):
                    innings = int(m.group(3))

            link = result_cell.css_first("a")
            if link:
                href = link.attrs.get("href") or ""
                m = re.search(r"/contests/(\d+)/box_score", href)
                if m:
                    game_id = int(m.group(1))

            records.append(
                {
                    "date": date,
                    "opponent_raw": opponent_raw,
                    "opponent_name": opponent_name,
                    "opponent_id": opponent_id,
                    "opponent_rank": None,
                    "opponent_site": opponent_site,
                    "opponent_note": opponent_note,
                    "result": result,
                    "team_score": team_score,
                    "opp_score": opp_score,
                    "innings": innings,
                    "attendance": None,  # no attendance available pre-2018
                    "game_id": game_id,
                }
            )

        return pl.DataFrame(records)

    elif num_cols >= 4:
        logger.info("📅 Detected modern schedule format (attendance column present)")
        tbody = table.css_first("tbody")
        if tbody is None:
            logger.warning(f"❌ Schedule table has no <tbody> at {season_url}")
            return pl.DataFrame()

        rows = tbody.css("tr.underline_rows")
        records = []

        for row in rows:
            cells = row.css("td")
            if len(cells) < 4:
                continue

            date = cells[0].text(strip=True)

            # Opponent breakdown
            opp = cells[1]
            opponent_raw = opp.text(strip=True)
            opponent_id = None
            opponent_rank = None
            opponent_name = opponent_raw
            opponent_site = ""
            opponent_note = ""

            a_tag = opp.css_first("a")
            if a_tag:
                href = a_tag.attrs.get("href") or ""
                m = re.search(r"/teams/(\d+)", href)
                if m:
                    opponent_id = int(m.group(1))

                anchor_text = a_tag.text(strip=True)

                # Try to extract opponent rank from anchor_text
                rank_match = re.match(r"#(\d+)\s+(.*)", anchor_text)
                if rank_match:
                    opponent_rank = int(rank_match.group(1))
                    opponent_name = rank_match.group(2)
                else:
                    opponent_name = anchor_text

                # Check for @ or vs prefix in raw text (not anchor)
                prefix_match = re.match(r"(@|vs)?\s*#?(\d+)?\s*", opponent_raw)
                if prefix_match:
                    if prefix_match.group(1):
                        opponent_site = prefix_match.group(1)

                # Remove known pieces to extract note
                cleaned = opponent_raw
                cleaned = re.sub(rf"^{re.escape(opponent_site)}", "", cleaned).strip()
                cleaned = re.sub(r"#\d+", "", cleaned).strip()
                cleaned = cleaned.replace(opponent_name, "", 1).strip()
                if cleaned:
                    opponent_note = cleaned

            # Result and game_id
            result = cells[2].text(strip=True)
            team_score, opp_score, innings = None, None, None
            m = re.search(r"[WL] (\d+)-(\d+)(?: \((\d+)\))?", result)
            if m:
                team_score = int(m.group(1))
                opp_score = int(m.group(2))
                if m.group(3):
                    innings = int(m.group(3))

            game_id = None
            result_link = cells[2].css_first("a")
            if result_link:
                href = result_link.attrs.get("href") or ""
                m = re.search(r"/contests/(\d+)/box_score", href)
                if m:
                    game_id = int(m.group(1))

            att_raw = cells[3].text(strip=True).replace(",", "")
            try:
                attendance = int(att_raw)
            except ValueError:
                attendance = None

            record = {
                "date": date,
                "opponent_raw": opponent_raw,
                "opponent_name": opponent_name,
                "opponent_id": opponent_id,
                "opponent_rank": opponent_rank,
                "opponent_site": opponent_site,
                "opponent_note": opponent_note,
                "result": result,
                "team_score": team_score,
                "opp_score": opp_score,
                "innings": innings,
                "attendance": attendance,
                "game_id": game_id,
            }
            records.append(record)

        return pl.DataFrame(records)
    else:
        logger.warning(f"⚠️ Unrecognized schedule format: {num_cols} columns")
        return pl.DataFrame()


def get_team_schedule(season_url: str) -> pl.DataFrame:
    """
    Scrape the schedule/results table for a given team season page.

    Args:
        season_url: Full URL to the team's season page (e.g., https://stats.ncaa.org/teams/596721)

    Returns:
        Polars DataFrame with schedule and result metadata.
    """
    dom: Optional[HTMLParser] = get_dom(f"{BASE_DOMAIN}{season_url}")
    if dom is None or dom.root is None:
        logger.error(f"❌ Failed to load DOM from {season_url}")
        return pl.DataFrame()

    df = _parse_schedule_dom(dom, season_url)
    logger.info(f"Parsed {df.shape[0]} games from {season_url}")
    if df.shape[0] > 0:
        logger.debug(f"First game: {df[0]}")
    return df


def get_pending_schedule_targets(limit: Optional[int] = None) -> pl.DataFrame:
    """
    Get team-season rows with has_schedule == True and not yet scraped.

    Returns:
        Polars DataFrame with columns:
        ["org_id", "school_name", "season_url", "year"]
    """
    # Connect to target DB and create log table if missing
    con = duckdb.connect(SEASON_SCHEDULE_DB)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEDULE_LOG_TABLE} (
            org_id INTEGER,
            school_name TEXT,
            season_url TEXT,
            success BOOLEAN,
            n_games INTEGER,
            error TEXT
        );
    """)
    scraped_urls = con.sql(
        f"SELECT DISTINCT season_url FROM {SCHEDULE_LOG_TABLE}"
    ).fetchall()
    already_done = {row[0] for row in scraped_urls}
    con.close()

    # Load from audit DB
    con_audit = duckdb.connect(INFO_DB_PATH)
    df = con_audit.sql("""
        SELECT org_id, school_name, season_url, year
        FROM season_info
        WHERE has_schedule = TRUE
    """).pl()
    con_audit.close()

    # Exclude completed URLs
    pending = df.filter(~pl.col("season_url").is_in(already_done))

    if limit:
        pending = pending.head(limit)

    return pending


def log_scrape_result(
    org_id: int,
    school_name: str,
    season_url: str,
    success: bool,
    n_games: int,
    error: Optional[str] = None,
) -> None:
    """
    Append one row to the log table to record scrape attempt.

    Args:
        org_id: Team org_id
        school_name: Full school name
        season_url: Unique identifier for team-season
        success: Whether schedule scrape succeeded
        n_games: Number of games parsed (0 if failed)
        error: Optional error message on failure
    """
    con = duckdb.connect(SEASON_SCHEDULE_DB)

    con.execute(
        f"""
        INSERT INTO {SCHEDULE_LOG_TABLE} (
            org_id, school_name, season_url, success, n_games, error
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (org_id, school_name, season_url, success, n_games, error),
    )

    con.close()


def append_schedule_data(
    df: pl.DataFrame,
    org_id: int,
    school_name: str,
    season_url: str,
    year: str,
) -> None:
    """
    Append schedule data to DuckDB with season context.

    Args:
        df: Game-level schedule records from get_team_schedule()
        org_id: School org_id
        school_name: Full school name
        season_url: Source URL (used for joining and tracing)
        year: Season year label
    """
    if df.is_empty():
        return

    df = df.with_columns(
        org_id=pl.lit(org_id, dtype=pl.Int32),
        school_name=pl.lit(school_name, dtype=pl.String),
        season_url=pl.lit(season_url, dtype=pl.String),
        year=pl.lit(year, dtype=pl.String),
    )

    con = duckdb.connect(SEASON_SCHEDULE_DB)

    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {SCHEDULE_DATA_TABLE} AS
        SELECT * FROM df LIMIT 0
    """)
    con.register("df", df)
    con.sql(f"INSERT INTO {SCHEDULE_DATA_TABLE} SELECT * FROM df")
    con.unregister("df")
    con.close()


def batch_scrape_team_schedules(limit: Optional[int] = None) -> None:
    """
    Batch scrape team schedules from season URLs with resume logic.

    Only scrapes if:
    - has_schedule = True
    - season_url not in log table

    Args:
        limit: Optional limit to number of team-seasons to process
    """
    logger.info(f"\n🚦 Starting batch scrape of team schedules (limit={limit})")
    pending = get_pending_schedule_targets(limit)

    if pending.is_empty():
        logger.info("📭 Nothing to scrape — all season schedules are logged.")
        return

    for row in pending.iter_rows(named=True):
        org_id = row["org_id"]
        school_name = row["school_name"]
        season_url = row["season_url"]
        year = row["year"]

        logger.info(f"\n🔍 {school_name} {year} — {season_url}")

        try:
            df = get_team_schedule(season_url)
            n_games = df.shape[0]
            append_schedule_data(df, org_id, school_name, season_url, year)
            log_scrape_result(
                org_id=org_id,
                school_name=school_name,
                season_url=season_url,
                success=True,
                n_games=n_games,
            )
            logger.info(f"✅ Scraped {n_games} games.")

        except Exception as e:
            logger.error(f"❌ Failed: {e}")
            log_scrape_result(
                org_id=org_id,
                school_name=school_name,
                season_url=season_url,
                success=False,
                n_games=0,
                error=str(e),
            )


if __name__ == "__main__":
    batch_scrape_team_schedules(limit=1000)
