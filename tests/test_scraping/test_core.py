import pytest
from selectolax.parser import HTMLParser

from batboy.scraping.core import get_dom, get_driver, make_request, throttle_and_retry

# Use a stable, static page
EXAMPLE_STATIC_URL = "https://httpbin.org/html"
EXAMPLE_JS_URL = "https://stats.ncaa.org/teams/history?org_id=2&sport_code=MBA"


@pytest.mark.web
def test_make_request_returns_200():
    resp = make_request(EXAMPLE_STATIC_URL)
    assert resp.status_code == 200
    assert "<html>" in resp.text


@pytest.mark.web
def test_get_dom_returns_htmlparser():
    dom = get_dom(EXAMPLE_JS_URL, headless=True, delay=2.0)
    assert dom is not None
    assert isinstance(dom, HTMLParser)
    assert dom.css_first("title") is not None


@pytest.mark.web
def test_get_driver_initializes_quietly():
    driver = get_driver(headless=True)
    driver.get("https://example.com")
    assert "Example Domain" in driver.page_source
    driver.quit()


def test_throttle_and_retry_retries_then_succeeds(caplog):
    state = {"attempts": 0}

    def flaky_func():
        state["attempts"] += 1
        if state["attempts"] < 3:
            raise ValueError("temporary fail")
        return "success"

    result = throttle_and_retry(flaky_func, max_retries=5, verbose=False)
    assert result == "success"
    assert state["attempts"] == 3
