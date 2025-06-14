import logging
import random
import time
from typing import Optional

import requests
from selectolax.parser import HTMLParser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth

from batboy.config.constants import HEADERS


def make_request(url: str, timeout: float = 10.0) -> requests.Response:
    """Static HTML request with custom headers."""
    return requests.get(url, headers=HEADERS, timeout=timeout)


def get_driver(headless: bool = True) -> webdriver.Chrome:
    """Return a stealth-patched Chrome driver."""
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    stealth(
        driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    return driver


def get_dom(
    url: str,
    delay: float = 2.0,
    headless: bool = True,
    min_delay: float = 0.5,
    max_delay: float = 1.5,
    max_retries: int = 3,
    verbose: bool = True,
) -> Optional[HTMLParser]:
    """Selenium + stealth + retries to render JS and return parsed DOM."""

    def fetch():
        driver = get_driver(headless=headless)
        driver.get(url)
        time.sleep(delay)
        html = driver.page_source
        driver.quit()
        return HTMLParser(html)

    return throttle_and_retry(fetch, max_retries, min_delay, max_delay, verbose)


def throttle_and_retry(
    func,
    max_retries: int = 3,
    min_delay: float = 1.0,
    max_delay: float = 2.5,
    verbose: bool = True,
):
    """Wrap a request function with delay and retry logic."""
    attempt = 0
    while attempt < max_retries:
        delay = random.uniform(min_delay, max_delay)
        if verbose:
            logging.info(f"Waiting {delay:.2f}s before request...")
        time.sleep(delay)
        try:
            return func()
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                raise RuntimeError(f"Failed after {max_retries} attempts: {e}")
            backoff = 2**attempt
            logging.warning(f"Attempt {attempt} failed, retrying in {backoff}s...")
            time.sleep(backoff)
