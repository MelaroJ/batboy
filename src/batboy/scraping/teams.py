import re
import time
from pathlib import Path
from typing import Optional, Union

import polars as pl
from rich.pretty import pprint
from selectolax.parser import HTMLParser

from batboy.config.constants import BASE_DOMAIN
from batboy.data import load_schools
from batboy.scraping.core import get_dom, get_driver
from batboy.utils import setup_logger

logger = setup_logger()

DATA_PATH = Path(__file__).parent.parent / "data" / "ncaa_schools.parquet"


def get_ncaa_baseball_teams(refresh: bool = False) -> pl.DataFrame:
    """
    Scrape org_id and school name from the NCAA baseball team dropdown.

    If the local parquet file exists and refresh=False, it loads from disk.
    Otherwise, it scrapes from the website and updates the local file.

    Returns:
        pl.DataFrame with columns: "org_id", "school_name"
    """
    if DATA_PATH.exists() and not refresh:
        logger.info(f"Loading cached teams from {DATA_PATH}")
        return pl.read_parquet(DATA_PATH)
    else:
        logger.info("Refreshing NCAA baseball teams list.")

    logger.info(f"Fetching list of NCAA baseball teams at {BASE_DOMAIN}/teams/history")

    dom: Optional[HTMLParser] = get_dom(f"{BASE_DOMAIN}/teams/history")
    if dom is None or dom.root is None:
        raise ValueError("get_dom() did not return a valid HTML document")

    select = dom.css_first('select[name="org_id"]')
    if not select:
        logger.warning("No select[name='org_id'] element found.")
        return pl.DataFrame()

    options = select.css("option")
    rows = []
    for opt in options:
        value = opt.attrs.get("value")
        name = opt.text(strip=True)
        if value and name and value.isdigit():
            rows.append({"org_id": int(value), "school_name": name})

    df = pl.DataFrame(rows)
    logger.info(f"Found {df.shape[0]} teams. Saving to {DATA_PATH}")
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(DATA_PATH)

    return df


def get_team_seasons(team: Union[int, str]) -> pl.DataFrame:
    """
    Scrape all seasons for a given NCAA baseball team.
    Handles pagination and expands result beyond 25 default entries.

    Accepts either org_id (int) or school name (str).
    """
    df = load_schools()

    # Convert stringified numbers to int
    if isinstance(team, str) and team.isdigit():
        team = int(team)

    if isinstance(team, str):
        match = df.filter(pl.col("school_name") == team)
        if match.shape[0] != 1:
            raise ValueError(f"School name '{team}' not found or ambiguous.")
        org_id = match[0, "org_id"]
        team_label = team
    else:
        org_id = team
        match = df.filter(pl.col("org_id") == org_id)
        team_label = (
            match[0, "school_name"] if match.shape[0] == 1 else f"org_id={org_id}"
        )

    url = f"{BASE_DOMAIN}/teams/history?org_id={org_id}&sport_code=MBA"
    logger.info(f"Fetching seasons for {team_label} ({org_id}) from {url}")

    # Use a live Selenium driver to interact with dropdown and pagination
    driver = get_driver(headless=True)
    driver.get(url)
    time.sleep(2.0)

    # Select 100 entries per page
    try:
        select = driver.find_element("name", "team_history_data_table_length")
        for option in select.find_elements("tag name", "option"):
            if option.get_attribute("value") == "100":
                option.click()
                time.sleep(2.0)  # Allow time for table to reload
                break
    except Exception as e:
        driver.quit()
        raise RuntimeError(f"Failed to select page length dropdown: {e}")

    records = []

    while True:
        html = driver.page_source
        dom = HTMLParser(html)
        table = dom.css_first("#team_history_data_table")
        if not table:
            driver.quit()
            raise ValueError("No team history table found in DOM.")

        tbody = table.css_first("tbody")
        if not tbody:
            driver.quit()
            raise ValueError("Table missing <tbody>.")

        for row in tbody.css("tr"):
            cells = row.css("td")
            if len(cells) < 9:
                continue

            season_id = None
            season_url = None
            year_cell = cells[0]
            year_label = year_cell.text(strip=True)

            year_link = year_cell.css_first("a")
            if year_link:
                href = year_link.attributes.get("href")
                if isinstance(href, str):
                    season_url = href
                    match = re.search(r"/teams/(\d+)", href)
                    if match:
                        season_id = int(match.group(1))

            try:
                record = {
                    "org_id": org_id,
                    "season_id": season_id,
                    "season_url": season_url,
                    "year": year_label,
                    "coach": cells[1].text(strip=True),
                    "division": cells[2].text(strip=True),
                    "conference": cells[3].text(strip=True),
                    "wins": int(cells[4].text(strip=True)),
                    "losses": int(cells[5].text(strip=True)),
                    "ties": int(cells[6].text(strip=True)),
                    "win_pct": float(cells[7].text(strip=True)),
                    "notes": cells[8].text(strip=True),
                }
                records.append(record)
            except Exception:
                continue

        # Try to click next page
        try:
            next_btn = driver.find_element(
                "css selector", "#team_history_data_table_next"
            )
            class_attr = next_btn.get_attribute("class") or ""
            if "disabled" in class_attr:
                break  # Exit loop
            else:
                next_btn.click()
                time.sleep(2.0)  # Wait for next page to load
        except Exception:
            logger.warning("Pagination failed or ended early.")
            break

    driver.quit()

    if records:
        first_year = records[-1]["year"]
        last_year = records[0]["year"]
        logger.info(f"Most recent season: {last_year}")
        pprint(records[0])
        logger.info(
            f"{team_label} history for {len(records)} seasons scraped ({first_year} to {
                last_year
            })."
        )
    else:
        logger.warning(f"No seasons found for {team_label}.")

    return pl.DataFrame(records)
