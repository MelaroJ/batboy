import pytest

from batboy.scraping.core import throttle_and_retry


def test_throttle_and_retry_successful_function():
    def dummy_func():
        return "ok"

    result = throttle_and_retry(dummy_func, max_retries=2, min_delay=0, max_delay=0)
    assert result == "ok"


def test_throttle_and_retry_fails():
    def always_fail():
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError, match="Failed after 2 attempts"):
        throttle_and_retry(always_fail, max_retries=2, min_delay=0, max_delay=0)
