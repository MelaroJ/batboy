import re
from typing import Optional, cast

import polars as pl
from bs4 import BeautifulSoup, Tag

from batboy.scraping.core import make_request
from batboy.utils import setup_logger

logger = setup_logger()


def extract_player_row(row: Tag, season_id: int) -> Optional[dict]:
    """Extract player metadata from a single <tr> in the roster table."""
    cells = row.find_all("td")
    if len(cells) != 11 or not all(isinstance(cell, Tag) for cell in cells):
        return None

    if not isinstance(cells[3], Tag):
        return None

    cell3: Tag = cast(Tag, cells[3])  # tell Pyright this is a Tag
    name_tag = cell3.find("a")
    if not isinstance(name_tag, Tag):
        return None

    name = name_tag.get_text(strip=True)
    href = name_tag.get("href")
    player_id = None
    player_url = None

    if isinstance(href, str):
        match = re.search(r"/players/(\d+)", href)
        if match:
            player_id = int(match.group(1))
            player_url = f"https://stats.ncaa.org{href}"

    try:
        return {
            "season_id": season_id,
            "player_id": player_id,
            "player_url": player_url,
            "name": name,
            "gp": int(cells[0].get_text(strip=True)),
            "gs": int(cells[1].get_text(strip=True)),
            "number": int(cells[2].get_text(strip=True)),
            "class": cells[4].get_text(strip=True),
            "position": cells[5].get_text(strip=True),
            "height": cells[6].get_text(strip=True),
            "bats": cells[7].get_text(strip=True),
            "throws": cells[8].get_text(strip=True),
            "hometown": cells[9].get_text(strip=True),
            "highschool": cells[10].get_text(strip=True),
        }
    except Exception as e:
        logger.warning(f"Failed to parse player row: {e}")
        return None


def scrape_roster(season_id: int, sort_by: Optional[str] = None) -> pl.DataFrame:
    """Scrape the roster for a given team season ID.

    Args:
        season_id: NCAA season identifier (from team season page).
        sort_by: Optional sort key — one of {"name", "number"}.

    Returns:
        Polars DataFrame with one row per player.
    """
    url = f"https://stats.ncaa.org/teams/{season_id}/roster"
    logger.info(f"Fetching roster for season_id={season_id}...")

    response = make_request(url)
    soup = BeautifulSoup(response.text, "lxml")

    table = soup.find("table", id=re.compile(r"rosters_form_players_\d+_data_table"))
    if not isinstance(table, Tag):
        raise ValueError("Could not find roster table on the page.")

    tbody = table.find("tbody")
    if not isinstance(tbody, Tag):
        raise ValueError("Roster table missing <tbody>.")

    rows = [r for r in tbody.find_all("tr") if isinstance(r, Tag)]
    logger.info(f"Found {len(rows)} player rows")

    records = [extract_player_row(row, season_id) for row in rows]
    records = [r for r in records if r is not None]

    df = pl.DataFrame(records)

    if sort_by:
        if sort_by not in {"name", "number"}:
            raise ValueError("sort_by must be one of {'name', 'number'}")
        df = df.sort(sort_by)

    logger.info(f"Parsed {len(df)} players for season_id={season_id}")
    return df
