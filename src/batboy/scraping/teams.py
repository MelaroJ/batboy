import re
import time
from typing import Optional

import polars as pl
from bs4 import BeautifulSoup, Tag
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from batboy.scraping.core import get_driver, get_soup
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


# def get_team_seasons(org_id: int) -> pl.DataFrame:
#     """
#     Scrape all seasons for a given NCAA baseball team (org_id).
#     Returns one row per season with metadata.
#     """
#     soup: Optional[BeautifulSoup] = get_soup(
#         f"https://stats.ncaa.org/teams/history?org_id={org_id}&sport_code=MBA"
#     )
#     if not isinstance(soup, BeautifulSoup):
#         raise ValueError("Failed to load or parse HTML for team history.")
#
#     table = soup.find("table", {"id": "team_history_data_table"})
#     if not isinstance(table, Tag):
#         raise ValueError(f"No team history table found for org_id={org_id}")
#
#     _tbody = table.find("tbody")
#     if not isinstance(_tbody, Tag):
#         raise ValueError("Team history table missing <tbody>.")
#     tbody: Tag = _tbody  # type narrowing for Pyright
#
#     rows = tbody.find_all("tr")
#     records = []
#
#     for row in rows:
#         if not isinstance(row, Tag):
#             continue
#
#         cells = row.find_all("td")
#         if len(cells) < 9:
#             continue
#
#         year_cell = cells[0]
#         if not isinstance(year_cell, Tag):
#             continue
#
#         year_link = year_cell.find("a")
#
#         season_id = None
#         year_label = cells[0].get_text(strip=True)
#
#         if isinstance(year_link, Tag):
#             href = year_link.get("href")
#             if isinstance(href, str):
#                 match = re.search(r"/teams/(\d+)", href)
#                 if match:
#                     season_id = int(match.group(1))
#
#         try:
#             record = {
#                 "org_id": org_id,
#                 "season_id": season_id,
#                 "year": year_label,
#                 "coach": cells[1].get_text(strip=True),
#                 "division": cells[2].get_text(strip=True),
#                 "conference": cells[3].get_text(strip=True),
#                 "wins": int(cells[4].get_text(strip=True)),
#                 "losses": int(cells[5].get_text(strip=True)),
#                 "ties": int(cells[6].get_text(strip=True)),
#                 "win_pct": float(cells[7].get_text(strip=True)),
#                 "notes": cells[8].get_text(strip=True),
#             }
#             records.append(record)
#         except Exception:
#             continue
#
#     return pl.DataFrame(records)


def scrape_team_history_table(org_id: int) -> list[dict]:
    url = f"https://stats.ncaa.org/teams/history?org_id={org_id}&sport_code=MBA"
    logger.info(f"Loading team history page for org_id={org_id}")

    driver = get_driver(headless=True)
    driver.get(url)

    # Select "Show 100 entries"
    try:
        select = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "team_history_data_table_length"))
        )
        select.find_element(By.XPATH, ".//option[text()='100']").click()
        logger.info("Selected 100 entries per page")
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not select 100 entries: {e}")

    rows = []
    page = 1
    while True:
        logger.info(f"Scraping page {page}...")
        table = driver.find_element(By.ID, "team_history_data_table")
        tbody = table.find_element(By.TAG_NAME, "tbody")
        for row in tbody.find_elements(By.TAG_NAME, "tr"):
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 9:
                continue

            season_id = None
            try:
                year_link = cells[0].find_element(By.TAG_NAME, "a")
                href = year_link.get_attribute("href")
                if isinstance(href, str):
                    match = re.search(r"/teams/(\d+)", href)
                    if match:
                        season_id = int(match.group(1))
            except Exception:
                continue

            try:
                rows.append(
                    {
                        "org_id": org_id,
                        "season_id": season_id,
                        "year": cells[0].text.strip(),
                        "coach": cells[1].text.strip(),
                        "division": cells[2].text.strip(),
                        "conference": cells[3].text.strip(),
                        "wins": int(cells[4].text.strip()),
                        "losses": int(cells[5].text.strip()),
                        "ties": int(cells[6].text.strip()),
                        "win_pct": float(cells[7].text.strip()),
                        "notes": cells[8].text.strip(),
                    }
                )
            except Exception:
                continue

        try:
            next_button = driver.find_element(By.ID, "team_history_data_table_next")
            next_class = next_button.get_attribute("class") or ""
            if "disabled" in next_class:
                break
            next_button.click()
            page += 1
            time.sleep(2)
        except Exception:
            logger.info("No more pages or error during pagination.")
            break

    driver.quit()
    logger.info(f"Scraped {len(rows)} total seasons for org_id={org_id}")
    return rows


def get_team_seasons_from_rows(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)
