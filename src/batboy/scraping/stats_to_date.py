import re
from typing import Optional
from urllib.parse import parse_qs, urlparse

import polars as pl
from bs4 import BeautifulSoup, Tag

from batboy.scraping.core import make_request
from batboy.utils import setup_logger

logger = setup_logger()


def get_stat_category_ids(season_id: int) -> dict[str, Optional[int]]:
    """Parse the team stats tab UI and extract available year_stat_category_id values."""
    url = f"https://stats.ncaa.org/teams/{season_id}/season_to_date_stats"
    soup = BeautifulSoup(make_request(url).text, "lxml")

    # Find all nav-tab blocks
    # Only keep real Tags (not NavigableString, etc.)
    tab_blocks = [
        el
        for el in soup.find_all("ul", class_="nav nav-tabs padding-nav")
        if isinstance(el, Tag)
    ]

    if len(tab_blocks) < 2:
        raise ValueError("Could not locate stat-type tab block")

    stat_nav: Tag = tab_blocks[1]

    stat_types: dict[str, Optional[int]] = {"hitting": None}

    for li in stat_nav.find_all("li", class_="nav-item"):
        if not isinstance(li, Tag):
            continue
        a = li.find("a")
        if not isinstance(a, Tag):
            continue

        label = a.get_text(strip=True).lower()
        href = a.get("href")
        if not isinstance(href, str):
            continue

        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        if "year_stat_category_id" in qs:
            try:
                stat_id = int(qs["year_stat_category_id"][0])
                stat_types[label] = stat_id
            except (ValueError, IndexError):
                continue

    return stat_types


def scrape_team_stats(season_id: int, stat_type: str = "hitting") -> pl.DataFrame:
    """Scrape hitting, pitching, or fielding stats for a given team season."""
    stat_map = get_stat_category_ids(season_id)
    if stat_type not in stat_map:
        raise ValueError(
            f"'{stat_type}' not available for season {season_id}. Choose from {
                list(stat_map)
            }"
        )

    stat_id = stat_map[stat_type]
    base_url = f"https://stats.ncaa.org/teams/{season_id}/season_to_date_stats"
    url = f"{base_url}?year_stat_category_id={stat_id}" if stat_id else base_url
    soup = BeautifulSoup(make_request(url).text, "lxml")

    # Log the date header
    header_div = soup.find("div", class_="card-header")
    if isinstance(header_div, Tag):
        header_text = header_div.get_text(strip=True)
        logger.info(f"{stat_type.title()} Stats - {header_text}")

    table = soup.find("table")
    if not isinstance(table, Tag):
        raise ValueError(f"No stats table found for stat_type={stat_type}")

    thead = table.find("thead")
    tbody = table.find("tbody")
    if not isinstance(thead, Tag) or not isinstance(tbody, Tag):
        raise ValueError("Malformed stats table")

    headers = [th.get_text(strip=True).lower() for th in thead.find_all("th")]
    records = []

    for row in tbody.find_all("tr"):
        if not isinstance(row, Tag):
            continue

        cells = row.find_all("td")
        if len(cells) != len(headers):
            continue

        name_cell = cells[1]
        if not isinstance(name_cell, Tag):
            continue

        name_link = name_cell.find("a")
        name = (
            name_link.get_text(strip=True)
            if isinstance(name_link, Tag)
            else name_cell.get_text(strip=True)
        )
        href = name_link.get("href") if isinstance(name_link, Tag) else None
        player_id = None
        if isinstance(href, str):
            match = re.search(r"/players/(\d+)", href)
            if match:
                player_id = int(match.group(1))

        # Skip situational rows
        if not name.strip() and player_id is None:
            continue

        row_data = {
            "season_id": season_id,
            "player_id": player_id,
            "player_name": name,
            "player_url": f"https://stats.ncaa.org{href}"
            if isinstance(href, str)
            else None,
            "stat_type": stat_type,
        }

        for i, key in enumerate(headers):
            if i == 1:
                continue  # skip player name column
            value = cells[i].get_text(strip=True).replace(",", "").lower()
            row_data[key] = None if value in {"", "-", "--", "na", "n/a"} else value

        records.append(row_data)

    df = pl.DataFrame(records)

    # Define field type rules
    float_fields = {"ba", "obpct", "slgpct", "era", "fldpct", "sbapct"}
    int_fields = {
        "2b",
        "2b-a",
        "3b",
        "3b-a",
        "a",
        "ab",
        "app",
        "bb",
        "bf",
        "bk",
        "cg",
        "ci",
        "cs",
        "csb",
        "e",
        "er",
        "fo",
        "gdp",
        "go",
        "gp",
        "gs",
        "h",
        "hb",
        "hbp",
        "hr",
        "hr-a",
        "ibb",
        "idp",
        "inh run score",
        "inh run",
        "ip",
        "k",
        "kl",
        "l",
        "opp dp",
        "p-oab",
        "pb",
        "picked",
        "pickoffs",
        "pitches",
        "po",
        "r",
        "rbi",
        "rbi2out",
        "sb",
        "sba",
        "sf",
        "sfa",
        "sh",
        "sha",
        "sho",
        "so",
        "sv",
        "tb",
        "tc",
        "w",
        "wp",
    }

    for col in df.columns:
        if col in float_fields:
            df = df.with_columns(pl.col(col).cast(pl.Float32, strict=False))
        elif col in int_fields:
            df = df.with_columns(pl.col(col).cast(pl.Int16, strict=False))

    return df
