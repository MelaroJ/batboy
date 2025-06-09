from typing import Optional

import polars as pl
from bs4 import BeautifulSoup, Tag

from batboy.scraping.core import get_soup
from batboy.utils import setup_logger

logger = setup_logger()


def get_ncaa_baseball_teams() -> pl.DataFrame:
    """
    Scrape org_id and school name from the NCAA baseball team dropdown.
    Returns:
        pl.DataFrame with columns: "org_id", "school_name"
    """
    logger.info("Fetching list of NCAA baseball teams...")
    soup_result: Optional[BeautifulSoup] = get_soup(
        "https://stats.ncaa.org/teams/history"
    )
    if not isinstance(soup_result, BeautifulSoup):
        raise ValueError("get_soup() did not return a valid BeautifulSoup object")

    select_tag = soup_result.find("select", id="org_id_select")
    if not isinstance(select_tag, Tag):
        raise ValueError("Could not find #org_id_select dropdown")

    options = select_tag.find_all("option")

    org_ids: list[int] = []
    school_names: list[str] = []

    for option in options:
        if not isinstance(option, Tag):
            continue  # skip any NavigableString or weird edge cases

        value_raw = option.get("value")
        value = str(value_raw) if value_raw is not None else ""
        if value.isdigit():
            org_ids.append(int(value))
            school_names.append(option.get_text(strip=True))

    logger.info(f"Parsed {len(org_ids)} schools from dropdown.")

    return pl.DataFrame({"org_id": org_ids, "school_name": school_names})
