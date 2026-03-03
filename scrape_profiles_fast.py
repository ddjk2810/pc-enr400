"""
Fast Procore Profile Rescraper — Skips search, hits known URLs directly.

Reads the most recent CSV in data/ to get all ~958 known profile URLs,
re-scrapes each one, and saves results in the same 17-column format.
Prints a comparison report at the end.

Runtime: ~16 min (958 URLs × 1s delay) vs 60-90 min for full scraper.

Usage:
    python scrape_profiles_fast.py
"""

import csv
import glob
import re
import sys
import time
import json
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

# Configuration
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = DATA_DIR


def find_latest_baseline():
    """Find the most recent procore_data_*.csv in data/ directory."""
    csvs = sorted(DATA_DIR.glob("procore_data_*.csv"))
    if not csvs:
        print("ERROR: No baseline CSV found in data/ directory")
        sys.exit(1)
    return csvs[-1]


BASELINE_FILE = find_latest_baseline()
BASE_URL = "https://network.procore.com"
REQUEST_DELAY = 1.0
SAVE_EVERY = 50  # Save progress every N profiles

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

FIELDNAMES = [
    'rank', 'enr_contractor_name', 'search_term', 'procore_company_name',
    'location', 'city', 'state', 'address', 'profile_url', 'joined_date',
    'total_projects', 'active_projects', 'avg_project_size', 'procore_users',
    'business_types', 'market_sectors', 'match_confidence'
]


def format_date(date_str):
    """Convert ISO date to Month Year format."""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime('%B %Y')
    except Exception:
        return date_str


def scrape_profile(session, profile_url):
    """Scrape a contractor profile page for metrics."""
    try:
        response = session.get(profile_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()

        data = {
            'profile_url': profile_url,
            'company_name': None,
            'location': None,
            'city': None,
            'state': None,
            'address': None,
            'joined_date': None,
            'total_projects': None,
            'active_projects': None,
            'avg_project_size': None,
            'procore_users': None,
            'business_types': None,
            'market_sectors': None,
        }

        # Extract from __NEXT_DATA__ JSON
        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if next_data_script:
            try:
                json_data = json.loads(next_data_script.string)
                business = json_data.get('props', {}).get('pageProps', {}).get('business', {})

                if business:
                    data['company_name'] = business.get('name')

                    primary_addr = business.get('primaryAddress', {})
                    data['city'] = primary_addr.get('city')
                    data['state'] = primary_addr.get('province')
                    data['address'] = primary_addr.get('display', {}).get('address')
                    if data['city'] and data['state']:
                        data['location'] = f"{data['city']}, {data['state']}"

                    created_at = business.get('monolithCreatedAt') or business.get('createdAt')
                    if created_at:
                        data['joined_date'] = format_date(created_at)

                    business_types = business.get('businessTypes', [])
                    if business_types:
                        data['business_types'] = ', '.join(business_types)

                    sectors = business.get('constructionSectors', [])
                    if sectors:
                        data['market_sectors'] = ', '.join(sectors)
            except json.JSONDecodeError:
                pass

        # Extract metrics from page text
        total_match = re.search(r'(\d+)\s*Total Procore Projects?', text, re.IGNORECASE)
        if total_match:
            data['total_projects'] = int(total_match.group(1))

        active_match = re.search(r'(\d+)\s*Active Procore Projects?', text, re.IGNORECASE)
        if active_match:
            data['active_projects'] = int(active_match.group(1))

        avg_match = re.search(r'\$?([\d,]+)\s*Average Procore Project Size', text, re.IGNORECASE)
        if avg_match:
            data['avg_project_size'] = int(avg_match.group(1).replace(',', ''))

        users_match = re.search(r'(\d+)\s*Procore Users?', text, re.IGNORECASE)
        if users_match:
            data['procore_users'] = int(users_match.group(1))

        return data
    except Exception as e:
        print(f"    Error scraping profile: {e}")
        return None


def load_baseline():
    """Load baseline CSV and return rows with profile URLs."""
    with open(BASELINE_FILE, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    profiles = [r for r in rows if r['profile_url'] and r['procore_company_name'] != 'NO MATCH FOUND']
    no_match = [r for r in rows if r['procore_company_name'] == 'NO MATCH FOUND']
    return profiles, no_match, rows


def save_results(output_file, rows):
    """Save current results to CSV."""
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def compare_results(baseline_data, new_data):
    """Compare new results to baseline and report changes."""
    def aggregate(data):
        contractors = {}
        for r in data:
            if r['procore_company_name'] == 'NO MATCH FOUND':
                continue
            rank = r['rank']
            if rank not in contractors:
                contractors[rank] = {'name': r['enr_contractor_name'], 'locations': 0, 'projects': 0}
            contractors[rank]['locations'] += 1
            if r['total_projects']:
                contractors[rank]['projects'] += int(r['total_projects'])
        return contractors

    old = aggregate(baseline_data)
    new = aggregate(new_data)

    old_ranks = set(old.keys())
    new_ranks = set(new.keys())

    # Lost profiles (URL no longer works)
    lost = old_ranks - new_ranks
    if lost:
        print(f"\n  LOST PROFILES ({len(lost)} contractors):")
        for rank in sorted(lost, key=lambda x: int(x)):
            print(f"    Rank {rank}: {old[rank]['name']}")

    # Project changes
    increased = []
    decreased = []
    unchanged = []

    for rank in old_ranks & new_ranks:
        old_proj = old[rank]['projects']
        new_proj = new[rank]['projects']
        diff = new_proj - old_proj

        if diff > 0:
            increased.append((rank, old[rank]['name'], old_proj, new_proj, diff))
        elif diff < 0:
            decreased.append((rank, old[rank]['name'], old_proj, new_proj, diff))
        else:
            unchanged.append((rank, old[rank]['name'], old_proj))

    increased.sort(key=lambda x: x[4], reverse=True)
    decreased.sort(key=lambda x: x[4])

    print(f"\n  PROJECT INCREASES ({len(increased)} contractors):")
    if increased:
        for rank, name, old_p, new_p, diff in increased[:30]:
            print(f"    Rank {rank} {name[:30]}: {old_p} -> {new_p} (+{diff})")
        if len(increased) > 30:
            print(f"    ... and {len(increased) - 30} more")
    else:
        print(f"    None")

    print(f"\n  PROJECT DECREASES ({len(decreased)} contractors):")
    if decreased:
        for rank, name, old_p, new_p, diff in decreased[:30]:
            print(f"    Rank {rank} {name[:30]}: {old_p} -> {new_p} ({diff})")
        if len(decreased) > 30:
            print(f"    ... and {len(decreased) - 30} more")
    else:
        print(f"    None")

    print(f"\n  UNCHANGED ({len(unchanged)} contractors)")

    # Location-level changes
    old_urls = {r['profile_url'] for r in baseline_data if r['procore_company_name'] != 'NO MATCH FOUND'}
    new_urls = {r['profile_url'] for r in new_data if r['procore_company_name'] != 'NO MATCH FOUND'}
    dead_urls = old_urls - new_urls
    if dead_urls:
        print(f"\n  DEAD PROFILE URLs ({len(dead_urls)}):")
        for url in sorted(dead_urls)[:20]:
            print(f"    {url}")
        if len(dead_urls) > 20:
            print(f"    ... and {len(dead_urls) - 20} more")


def main():
    """Main function: load baseline URLs, re-scrape, save, compare."""
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = OUTPUT_DIR / f"procore_data_{today}.csv"

    print("=" * 70)
    print("Procore ENR 400 — Fast Profile Rescraper")
    print(f"Date: {today}")
    print(f"Baseline: {BASELINE_FILE.name}")
    print("=" * 70)

    # Load baseline
    profiles, no_match_rows, all_baseline = load_baseline()
    print(f"\nLoaded {len(profiles)} profile URLs from baseline")
    print(f"Skipping {len(no_match_rows)} NO MATCH FOUND entries")

    # Check for resume file
    output_rows = []
    start_idx = 0
    if output_file.exists():
        with open(output_file, 'r', encoding='utf-8') as f:
            existing = list(csv.DictReader(f))
        scraped_urls = {r['profile_url'] for r in existing}
        # Find how many we already scraped
        already_done = sum(1 for p in profiles if p['profile_url'] in scraped_urls)
        if already_done > 0:
            print(f"Found existing output with {already_done}/{len(profiles)} profiles — resuming")
            output_rows = existing
            start_idx = already_done

    # Scrape profiles
    session = requests.Session()
    errors = 0

    for i in range(start_idx, len(profiles)):
        row = profiles[i]
        url = row['profile_url']
        rank = row['rank']
        enr_name = row['enr_contractor_name']

        print(f"\n[{i+1}/{len(profiles)}] Rank {rank}: {enr_name}")
        print(f"  URL: {url}")

        time.sleep(REQUEST_DELAY)
        profile_data = scrape_profile(session, url)

        if profile_data:
            output_rows.append({
                'rank': rank,
                'enr_contractor_name': enr_name,
                'search_term': row['search_term'],
                'procore_company_name': profile_data['company_name'] or '',
                'location': profile_data['location'] or '',
                'city': profile_data['city'] or '',
                'state': profile_data['state'] or '',
                'address': profile_data['address'] or '',
                'profile_url': profile_data['profile_url'],
                'joined_date': profile_data['joined_date'] or '',
                'total_projects': profile_data['total_projects'] if profile_data['total_projects'] is not None else '',
                'active_projects': profile_data['active_projects'] if profile_data['active_projects'] is not None else '',
                'avg_project_size': profile_data['avg_project_size'] if profile_data['avg_project_size'] is not None else '',
                'procore_users': profile_data['procore_users'] if profile_data['procore_users'] is not None else '',
                'business_types': profile_data['business_types'] or '',
                'market_sectors': profile_data['market_sectors'] or '',
                'match_confidence': 1.0,
            })
            proj_str = f"projects={profile_data['total_projects']}" if profile_data['total_projects'] else "no project data"
            print(f"    OK: {profile_data['company_name']} — {proj_str}")
        else:
            errors += 1
            # Keep baseline row but mark as failed
            output_rows.append({
                'rank': rank,
                'enr_contractor_name': enr_name,
                'search_term': row['search_term'],
                'procore_company_name': 'SCRAPE_FAILED',
                'location': '', 'city': '', 'state': '', 'address': '',
                'profile_url': url, 'joined_date': '', 'total_projects': '',
                'active_projects': '', 'avg_project_size': '', 'procore_users': '',
                'business_types': '', 'market_sectors': '', 'match_confidence': 0,
            })
            print(f"    FAILED")

        # Save progress periodically
        if (i + 1) % SAVE_EVERY == 0:
            save_results(output_file, output_rows)
            print(f"\n  [Saved progress: {len(output_rows)} rows]")

    # Add back NO MATCH rows from baseline (unchanged)
    for row in no_match_rows:
        output_rows.append({k: row.get(k, '') for k in FIELDNAMES})

    # Final save
    save_results(output_file, output_rows)

    # Summary
    ok_rows = [r for r in output_rows if r['procore_company_name'] not in ('NO MATCH FOUND', 'SCRAPE_FAILED')]
    failed_rows = [r for r in output_rows if r['procore_company_name'] == 'SCRAPE_FAILED']
    total_projects = sum(int(r['total_projects']) for r in ok_rows if r['total_projects'])
    total_active = sum(int(r['active_projects']) for r in ok_rows if r['active_projects'])

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Profiles scraped: {len(ok_rows)}")
    print(f"  Failed: {len(failed_rows)}")
    print(f"  No match (baseline): {len(no_match_rows)}")
    print(f"  Total projects: {total_projects:,}")
    print(f"  Active projects: {total_active:,}")
    print(f"\n  Output: {output_file.name}")

    # Compare to baseline
    print("\n" + "=" * 70)
    print(f"CHANGES FROM BASELINE ({BASELINE_FILE.name})")
    print("=" * 70)
    compare_results(all_baseline, output_rows)

    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()
