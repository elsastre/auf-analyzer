import time
import requests

def get(url, retries=3, backoff=2):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(backoff * (i + 1))
