# src/batboy/scraping/game_logs.py

import re
import time

import polars as pl
from bs4 import BeautifulSoup, Tag
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from batboy.config.constants import (
    FLOAT32_COLUMNS,
    INT64_COLUMNS,
    STAT_CATEGORY_SCHEMAS,
)
from batboy.scraping.core import get_driver
from batboy.utils import setup_logger

logger = setup_logger()


def get_team_game_log_player_ids(season_id: int) -> dict[int, str]:
    """Get all player_ids listed in the Game By Game logs page for a given team season."""
    logger.info(f"Fetching game log player IDs for season_id={season_id}")
    url = f"https://stats.ncaa.org/teams/{season_id}"
    driver = get_driver(headless=True)
    driver.get(url)
    time.sleep(2)

    try:
        logger.debug("Locating 'Game By Game' link...")
        link = driver.find_element(By.PARTIAL_LINK_TEXT, "Game By Game")
        href = link.get_attribute("href") or ""
        match = re.search(r"/players/(\d+)", href)
        if not match:
            raise ValueError("No player_id found in Game By Game link")
        team_player_id = int(match.group(1))
        logger.info(f"Found team player_id={team_player_id}")
    except Exception as e:
        driver.quit()
        logger.error("Failed to extract team Game By Game link")
        raise RuntimeError(f"Could not extract team Game By Game link: {e}")

    url = f"https://stats.ncaa.org/players/{team_player_id}"
    logger.debug(f"Navigating to {url}")
    driver.get(url)

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, "player_id"))
    )

    player_select = driver.find_element(By.ID, "player_id")
    options = player_select.find_elements(By.TAG_NAME, "option")

    player_id_map = {}
    for option in options:
        value = option.get_attribute("value")
        if value is not None and value.isdigit():
            pid = int(value)
            name = option.text.strip()
            player_id_map[pid] = name
            logger.debug(f"Found player_id={pid}: {name}")

    logger.info(f"Discovered {len(player_id_map)} player IDs for season_id={season_id}")
    driver.quit()
    return player_id_map


def scrape_game_log(player_id: int, stat_type: str = "hitting") -> pl.DataFrame:
    """Scrape game-by-game log for a given player and stat category (hitting, pitching, fielding)."""
    logger.info(f"Scraping game log for player_id={player_id}, stat_type='{stat_type}'")
    assert stat_type in STAT_CATEGORY_SCHEMAS, f"Invalid stat_type: {stat_type}"

    url = f"https://stats.ncaa.org/players/{player_id}"
    driver = get_driver(headless=True)
    logger.debug(f"Opening {url}")
    driver.get(url)

    # If we want pitching or fielding, click the appropriate tab
    if stat_type != "hitting":
        logger.debug(f"Switching to stat category: {stat_type}")
        try:
            # Example logic — tab labels may vary slightly
            link_text = stat_type.capitalize()  # "Pitching", "Fielding"
            tab = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.LINK_TEXT, link_text))
            )
            tab.click()
            logger.debug(f"Clicked on {link_text} tab")
            wait_for_table_rows(driver, player_id)
        except Exception as e:
            driver.quit()
            raise RuntimeError(f"Failed to switch to stat_type='{stat_type}': {e}")

    # Wait for the game log table to load
    table_wrapper_id = f"game_log_{player_id}_player_wrapper"
    logger.debug(f"Waiting for #{table_wrapper_id}")
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, table_wrapper_id))
        )
    except Exception as e:
        driver.quit()
        raise RuntimeError(f"Timed out waiting for game log table: {e}")

    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "lxml")
    df = parse_game_log_table(soup, player_id, stat_type)

    logger.info(f"Scraped {df.shape[0]} rows for player_id={player_id}")
    return df


def wait_for_table_rows(driver, player_id: int, timeout: int = 20) -> None:
    """Wait for the game log table to be populated with <tr> rows."""
    table_id = f"game_log_{player_id}_player"

    for second in range(timeout):
        try:
            table = driver.find_element(By.ID, table_id)
            rows = table.find_elements(By.TAG_NAME, "tr")
            if len(rows) > 1:  # One row is likely a <thead> row — we want real data
                logger.debug(f"Found {len(rows)} <tr> rows after {second}s")
                return
        except Exception:
            pass
        time.sleep(1)

    raise TimeoutError(
        f"Table rows did not load for player_id={player_id} after {timeout}s"
    )


def parse_game_log_table(
    soup: BeautifulSoup, player_id: int, stat_type: str
) -> pl.DataFrame:
    """Parse game log table directly from the table without relying on <tbody>."""
    table_id = f"game_log_{player_id}_player"
    table = soup.find("table", id=table_id)

    if not isinstance(table, Tag):
        raise ValueError(f"Could not find game log table with id='{table_id}'")

    rows = []
    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue

        cells = tr.find_all("td")
        if not cells:
            continue

        record = {}
        for i, cell in enumerate(cells):
            if not isinstance(cell, Tag) or i >= len(STAT_CATEGORY_SCHEMAS[stat_type]):
                continue

            text = cell.get_text(strip=True).rstrip("/")

            # Extract opponent/team link (column 1)
            if i == 1:
                link = cell.find("a")
                if isinstance(link, Tag):
                    href = link.get("href")
                    if isinstance(href, str):
                        match = re.search(r"/teams/(\d+)", href)
                        if match:
                            record["opponent_id"] = int(match.group(1))

            # Extract result/box score (column 2)
            if i == 2:
                link = cell.find("a")
                if isinstance(link, Tag):
                    href = link.get("href")
                    if isinstance(href, str):
                        record["box_score_url"] = f"https://stats.ncaa.org{href}"
                        match = re.search(r"/contests/(\d+)/box_score", href)
                        if match:
                            record["game_id"] = int(match.group(1))

            header = STAT_CATEGORY_SCHEMAS[stat_type][i]
            record[header] = text

        rows.append(record)

    # Normalize to schema + extra fields
    columns = STAT_CATEGORY_SCHEMAS[stat_type] + [
        "opponent_id",
        "box_score_url",
        "game_id",
    ]
    df = pl.DataFrame([{col: row.get(col, None) for col in columns} for row in rows])

    # Drop summary rows by inspecting 'Date' field
    df = df.filter(~pl.col("Date").str.contains("Totals", literal=True))

    for col in df.columns:
        if col in INT64_COLUMNS:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))
        elif col in FLOAT32_COLUMNS:
            df = df.with_columns(pl.col(col).cast(pl.Float32, strict=False))

    return df
