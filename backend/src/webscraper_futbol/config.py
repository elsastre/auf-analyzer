BASE_URL = "https://fbref.com"
LEAGUE_CODE = "45"  # Uruguay

def standings_url(season: int) -> str:
    return f"{BASE_URL}/en/comps/{LEAGUE_CODE}/{season}/standings/"
