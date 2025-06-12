from pathlib import Path

import polars as pl
import pytest
from selectolax.parser import HTMLParser

from batboy.scraping.schedules import _parse_schedule_dom


@pytest.mark.no_web
def test_parse_single_schedule_row():
    html = """
    <div class="card">
        <div class="card-header">Schedule/Results</div>
        <div class="card-body">
            <table>
                <tbody>
                    <tr class="underline_rows">
                        <td>03/01/2025</td>
                        <td><a href="/teams/123456">#3 Oklahoma</a></td>
                        <td><a href="/contests/654321/box_score">W 5-4 (11)</a></td>
                        <td>6,789</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
    """
    dom = HTMLParser(html)
    df = _parse_schedule_dom(dom, "dummy_url")

    assert df.shape[0] == 1
    row = df.row(0, named=True)

    assert row["date"] == "03/01/2025"
    assert row["opponent_name"] == "Oklahoma"
    assert row["opponent_rank"] == 3
    assert row["game_id"] == 654321
    assert row["team_score"] == 5
    assert row["opp_score"] == 4
    assert row["innings"] == 11
    assert row["attendance"] == 6789


@pytest.mark.no_web
def test_parse_real_schedule_fixture():
    fixture_path = (
        Path(__file__).parent.parent
        / "fixtures"
        / "html"
        / "tennessee_2025_schedule.html"
    )
    html = fixture_path.read_text(encoding="utf-8")
    dom = HTMLParser(html)

    df = _parse_schedule_dom(dom, "test-fixture")

    assert df.shape[0] >= 20  # Make sure it captured the full schedule
    assert {"date", "opponent_name", "team_score", "game_id"} <= set(df.columns)
    assert (
        df.filter(pl.col("innings").is_not_null()).shape[0] > 0
    )  # Test extra innings parsing
