# src/batboy/scraping/schedule.py

import re
from typing import Optional

import polars as pl
from bs4 import BeautifulSoup, Tag

from batboy.scraping.core import get_soup
from batboy.utils import setup_logger

logger = setup_logger()


def extract_opponent(cell: object) -> tuple[Optional[str], Optional[int]]:
    if not isinstance(cell, Tag):
        return None, None
    tag = cell.find("a")
    if not isinstance(tag, Tag):
        return None, None
    name = tag.get_text(strip=True)
    href = tag.get("href")
    if not isinstance(href, str):
        return name, None
    match = re.search(r"/teams/(\d+)", href)
    opponent_id = int(match.group(1)) if match else None
    return name, opponent_id


def extract_result(
    cell: object,
) -> tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[int]]:
    if not isinstance(cell, Tag):
        return None, None, None, None, None
    tag = cell.find("a")
    if not isinstance(tag, Tag):
        return None, None, None, None, None

    result = tag.get_text(strip=True)
    href = tag.get("href")
    box_score_url = f"https://stats.ncaa.org{href}" if isinstance(href, str) else None

    game_id = None
    if isinstance(href, str):
        match = re.search(r"/contests/(\d+)/box_score", href)
        if match:
            game_id = int(match.group(1))

    team_score = opp_score = None
    match = re.search(r"(W|L|T)?\s*(\d+)-(\d+)", result)
    if match:
        team_score = int(match.group(2))
        opp_score = int(match.group(3))

    return result, box_score_url, game_id, team_score, opp_score


def scrape_schedule(season_id: int) -> pl.DataFrame:
    url = f"https://stats.ncaa.org/teams/{season_id}"
    logger.info(f"Fetching schedule for season_id={season_id}...")

    soup: Optional[BeautifulSoup] = get_soup(url)

    if not isinstance(soup, BeautifulSoup):
        raise RuntimeError(f"Failed to load schedule page for season_id={season_id}")

    logger.info("Parsing schedule table...")
    container = soup.find("div", class_="card-body")
    if not isinstance(container, Tag):
        raise ValueError("Could not find schedule container")

    table = container.find("table")
    if not isinstance(table, Tag):
        raise ValueError("Could not find schedule table")

    records = []

    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue
        if "underline_rows" not in (tr.get("class") or []):
            continue

        tds = tr.find_all("td")
        if len(tds) != 4 or not all(isinstance(td, Tag) for td in tds):
            continue

        date_text = tds[0].get_text(strip=True)
        opponent_name, opponent_id = extract_opponent(tds[1])
        result, box_score_url, game_id, team_score, opp_score = extract_result(tds[2])

        att_raw = tds[3].get_text(strip=True).replace(",", "")
        attendance = int(att_raw) if att_raw.isdigit() else None

        records.append(
            {
                "season_id": season_id,
                "game_id": game_id,
                "date": date_text,
                "opponent_name": opponent_name,
                "opponent_id": opponent_id,
                "result": result,
                "team_score": team_score,
                "opp_score": opp_score,
                "attendance": attendance,
                "box_score_url": box_score_url,
            }
        )

    logger.info(f"Finished scraping {len(records)} games for season_id = {season_id}")
    return pl.DataFrame(records)
