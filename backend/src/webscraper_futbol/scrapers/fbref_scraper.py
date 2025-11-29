import time, random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def get_table(url: str):
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    drv = webdriver.Chrome(service=service, options=opts)

    try:
        drv.get(url)
        time.sleep(random.uniform(2, 3))  # Esperar carga completa
        html = drv.page_source
        return BeautifulSoup(html, "html5lib")
    finally:
        drv.quit()


def extract_standings(soup: BeautifulSoup):
    table = soup.find("table")
    if not table:
        return []
    rows = []
    for tr in table.select("tbody tr"):
        cells = [c.get_text(strip=True) for c in tr.select("th,td")]
        if cells:
            rows.append(cells)
    return rows
