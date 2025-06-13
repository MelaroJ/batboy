# src/batboy/config/constants.py

# paths
BASE_DOMAIN = "https://stats.ncaa.org"
NCAA_SCHOOLS = "src/batboy/data/ncaa_schools.parquet"
SEASON_INFO_OUT = "src/batboy/data/season_info_available.parquet"
INFO_DB_PATH = "src/batboy/data/season_info_audit.duckdb"

# Duckdb tables
SEASON_INFO_TABLE_NAME = "season_info"


# Global headers for static requests
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}

TRACKED_TABS = {
    "Schedule/Results",
    "Roster",
    "Team Statistics",
    "Game By Game",
    "Ranking Summary",
}

STAT_CATEGORY_SCHEMAS = {
    "hitting": [
        "Date",
        "Opponent",
        "Result",
        "R",
        "AB",
        "H",
        "2B",
        "3B",
        "TB",
        "HR",
        "RBI",
        "BB",
        "HBP",
        "SF",
        "SH",
        "K",
        "OPP DP",
        "CS",
        "Picked",
        "SB",
        "IBB",
        "GDP",
        "RBI2out",
    ],
    "pitching": [
        "Date",
        "Opponent",
        "Result",
        "App",
        "ERA",
        "IP",
        "CG",
        "H",
        "R",
        "ER",
        "BB",
        "SO",
        "SHO",
        "BF",
        "P-OAB",
        "2B-A",
        "3B-A",
        "Bk",
        "HR-A",
        "WP",
        "HB",
        "IBB",
        "Inh Run",
        "Inh Run Score",
        "SHA",
        "SFA",
        "Pitches",
        "GO",
        "FO",
        "W",
        "L",
        "SV",
        "KL",
        "pickoffs",
    ],
    "fielding": [
        "Date",
        "Opponent",
        "Result",
        "PO",
        "A",
        "TC",
        "E",
        "FldPct",
        "CI",
        "PB",
        "SBA",
        "CSB",
        "IDP",
        "TP",
        "SBAPct",
    ],
}

# Minimal set of common numeric columns (safe to cast as int or float)
INT16_COLUMNS = {
    "R",
    "AB",
    "H",
    "2B",
    "3B",
    "TB",
    "HR",
    "RBI",
    "BB",
    "HBP",
    "SF",
    "SH",
    "K",
    "OPP DP",
    "CS",
    "Picked",
    "SB",
    "IBB",
    "GDP",
    "RBI2out",
    "App",
    "CG",
    "ER",
    "SO",
    "BF",
    "2B-A",
    "3B-A",
    "Bk",
    "HR-A",
    "WP",
    "HB",
    "Inh Run",
    "Inh Run Score",
    "SHA",
    "SFA",
    "Pitches",
    "GO",
    "FO",
    "W",
    "L",
    "SV",
    "KL",
    "pickoffs",
    "PO",
    "A",
    "TC",
    "E",
    "CI",
    "PB",
    "SBA",
    "CSB",
    "IDP",
    "TP",
}

FLOAT16_COLUMNS = {"FldPct", "SBAPct", "ERA", "OBPct", "SlgPct", "BA", "IP", "win_pct"}


ALL_STAT_COLUMNS = set().union(*STAT_CATEGORY_SCHEMAS.values())

DEFAULT_DTYPES = {
    **{col: "Int16" for col in INT16_COLUMNS},
    **{col: "Float16" for col in FLOAT16_COLUMNS},
}
