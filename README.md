# Procore ENR 400 Profile Tracker

Automated daily scraper that tracks Procore network data for ~958 ENR 400 contractor profiles. Runs via GitHub Actions and commits results to `data/`.

## How It Works

1. Reads the most recent `data/procore_data_*.csv` as a baseline (for profile URLs)
2. Re-scrapes each profile page on `network.procore.com`
3. Saves a new dated CSV to `data/`
4. Compares results to baseline and prints a change report

## Data Collected

Per contractor location: company name, city/state, address, profile URL, joined date, total projects, active projects, average project size, Procore users, business types, market sectors, match confidence.

## Usage

```bash
pip install -r requirements.txt
python scrape_profiles_fast.py
```

Runtime: ~16 minutes (958 URLs, 1s delay between requests).

## Automation

GitHub Actions runs daily at 6 AM UTC (see `.github/workflows/daily-scrape.yml`). Can also be triggered manually via `gh workflow run daily-scrape.yml`.
