import os
import sys
import time
import random
import csv
import requests
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

if not POLYGON_API_KEY:
    print("Error: POLYGON_API_KEY is not set. Create a .env with POLYGON_API_KEY=your_key")
    sys.exit(1)

LIMIT = 1000

def fetch_json(url: str, max_retries: int = 5) -> dict:
    for attempt in range(max_retries):
        response = requests.get(url, timeout=15)
        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            if retry_after is not None:
                try:
                    sleep_seconds = float(retry_after)
                except ValueError:
                    sleep_seconds = 2 ** (attempt + 1)
            else:
                sleep_seconds = 2 ** (attempt + 1)
            print(f"Rate limited (429). Backing off for {sleep_seconds:.1f}s...")
            time.sleep(sleep_seconds)
            continue
        response.raise_for_status()
        return response.json()
    raise requests.HTTPError(f"Exceeded retries due to repeated 429 responses for URL: {url}")

def append_api_key(url: str, api_key: str) -> str:
    separator = '&' if '?' in url else '?'
    return f"{url}{separator}apiKey={api_key}"

base_url = (
    f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true"
    f"&order=asc&limit={LIMIT}&sort=ticker"
)

url = append_api_key(base_url, POLYGON_API_KEY)

try:
    data = fetch_json(url)
except requests.HTTPError as http_err:
    print(f"HTTP error: {http_err}")
    sys.exit(1)
except requests.RequestException as req_err:
    print(f"Request error: {req_err}")
    sys.exit(1)

tickers = []

results = data.get('results', [])
if not isinstance(results, list):
    print("Unexpected API response: 'results' is missing or not a list")
    sys.exit(1)

print(f"Fetched {len(results)} tickers on first page")
tickers.extend(results)

print("Resting 12s to respect rate limits...")
time.sleep(12)

next_url = data.get('next_url')
seen_next_url = None
while next_url:
    if next_url == seen_next_url:
        print("next_url unchanged between iterations; stopping to avoid infinite loop")
        break
    seen_next_url = next_url
    print('requesting data at', next_url)
    try:
        data = fetch_json(append_api_key(next_url, POLYGON_API_KEY))
    except requests.HTTPError as http_err:
        print(f"HTTP error: {http_err}")
        break
    except requests.RequestException as req_err:
        print(f"Request error: {req_err}")
        break

    results = data.get('results', [])
    if not isinstance(results, list):
        print("Unexpected API response: 'results' is missing or not a list")
        break
    print(f"Fetched {len(results)} tickers on this page (total so far: {len(tickers) + len(results)})")
    tickers.extend(results)

    next_url = data.get('next_url')

    print("Resting 12s to respect rate limits...")
    time.sleep(12)
    
sample = results if len(results) <= 10 else random.sample(results, 10)
print(sample)

total_items = len(tickers)
unique_symbols = {t.get('ticker') for t in tickers if isinstance(t, dict) and t.get('ticker')}
print(f"Total items fetched: {total_items}")
print(f"Unique tickers: {len(unique_symbols)}")



example_ticker = {'ticker': 'XJUL', 
    'name': 'FT Vest U.S. Equity Enhance & Moderate Buffer ETF - July', 
    'market': 'stocks', 
    'locale': 'us', 
    'primary_exchange': 'BATS', 
    'type': 'ETF', 
    'active': True, 
    'currency_name': 'usd', 
    'cik': '0001667919', 
    'composite_figi': 'BBG01HCDMD97', 
    'share_class_figi': 'BBG01HCDMFT0', 
    'last_updated_utc': '2025-09-15T06:04:58.615984543Z'}

# Write CSV with the same schema as example_ticker
FIELDNAMES = list(example_ticker.keys())

output_path = os.path.join(os.getcwd(), 'tickers.csv')
with open(output_path, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
    writer.writeheader()
    for t in tickers:
        if not isinstance(t, dict):
            continue
        row = {key: t.get(key, '') for key in FIELDNAMES}
        writer.writerow(row)

print(f"Wrote CSV: {output_path}")
