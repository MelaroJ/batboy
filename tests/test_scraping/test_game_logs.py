# tests/test_scraping/test_game_logs.py

from batboy.scraping.game_logs import get_team_game_log_player_ids


def test_get_team_game_log_player_ids_returns_expected_structure():
    season_id = 596721  # 2024-2025 Tennessee Volunteers

    player_id_map = get_team_game_log_player_ids(season_id)

    assert isinstance(player_id_map, dict)
    assert all(isinstance(k, int) for k in player_id_map)
    assert all(isinstance(v, str) for v in player_id_map.values())
    assert len(player_id_map) > 5  # should have team + players

    # Check that team stats player_id is in the result
    # (Team = first <option>, ID 8903722 as of June 2025)
    team_player_id = 8903722
    assert team_player_id in player_id_map
    assert "Tennessee stats" in player_id_map[team_player_id]
