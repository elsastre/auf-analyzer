import argparse
import csv
import os
from .scrapers.fbref_scraper import get_table, extract_standings

URL = "https://fbref.com/en/comps/45/table/Primera-Division-Uruguay-Stats"

def main():
    soup = get_table(URL)
    rows = extract_standings(soup)

    # ✅ Crear carpeta data si no existe
    os.makedirs("data", exist_ok=True)

    out = "data/standings_uruguay.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)

    print(f"✅ Filas: {len(rows)} guardado en {out}")
    for r in rows[:5]:
        print(r)

if __name__ == "__main__":
    main()
