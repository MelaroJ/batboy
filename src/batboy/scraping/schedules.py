import re
from typing import Optional

import polars as pl
from selectolax.parser import HTMLParser

from batboy.config.constants import BASE_DOMAIN
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
    if header is None or header.parent is None:
        logger.warning(f"❌ Schedule header not found at {season_url}")
        return pl.DataFrame()

    table = header.parent.css_first("table")
    if table is None:
        logger.warning(f"❌ Schedule table not found at {season_url}")
        return pl.DataFrame()

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


def get_team_schedule(season_url: str) -> pl.DataFrame:
    """
    Scrape the schedule/results table for a given team season page.

    Args:
        season_url: Full URL to the team's season page (e.g., https://stats.ncaa.org/teams/596721)

    Returns:
        Polars DataFrame with schedule and result metadata.
    """
    dom: Optional[HTMLParser] = get_dom(season_url)
    if dom is None or dom.root is None:
        logger.error(f"❌ Failed to load DOM from {season_url}")
        return pl.DataFrame()

    df = _parse_schedule_dom(dom, season_url)
    logger.info(f"Parsed {df.shape[0]} games from {season_url}")
    if df.shape[0] > 0:
        logger.debug(f"First game: {df[0]}")
    return df


if __name__ == "__main__":
    test_url = f"{BASE_DOMAIN}/teams/596721"
    df = get_team_schedule(test_url)
    print(df)
