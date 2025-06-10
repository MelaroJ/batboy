# src/batboy/scraping/game_logs.py

import re
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

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
