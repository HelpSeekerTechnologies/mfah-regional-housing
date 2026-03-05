"""
mfah_gold_build.py -- Create all 20 Gold layer tables in client_projects.mfah on Databricks.
Source of truth: communityData{} from mfah-deploy/index.html (lines 3757-4717).

Usage:
    python mfah_gold_build.py <DATABRICKS_TOKEN>
"""
import subprocess
import json
import sys
import time

# ============================================================================
# CONFIG
# ============================================================================
HOST = "https://adb-1169784117228619.19.azuredatabricks.net"
WAREHOUSE_ID = "a7e9ada5cd37e1c7"
S = "client_projects.mfah"

TOKEN = sys.argv[1] if len(sys.argv) > 1 else None
if not TOKEN:
    print("Usage: python mfah_gold_build.py <DATABRICKS_TOKEN>")
    print("Generate at: Settings > Developer > Access Tokens")
    sys.exit(1)

# ============================================================================
# SQL EXECUTION (curl subprocess -- avoids urllib 403 on Windows)
# ============================================================================
def run_sql(label, sql, timeout=180):
    """Submit SQL via Databricks SQL Statements API, poll for results."""
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")

    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "0s"
    })
    cmd = [
        "curl", "-s", "-X", "POST",
        f"{HOST}/api/2.0/sql/statements",
        "-H", f"Authorization: Bearer {TOKEN}",
        "-H", "Content-Type: application/json",
        "-d", payload
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    try:
        d = json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"  SUBMIT ERROR: {r.stdout[:500]}")
        return None

    sid = d.get("statement_id")
    if not sid:
        st = d.get("status", {}).get("state", "?")
        if st == "SUCCEEDED":
            return _extract_status(d)
        print(f"  SUBMIT ERROR: {json.dumps(d, indent=2)[:500]}")
        return None

    t0 = time.time()
    while time.time() - t0 < timeout:
        cmd_poll = [
            "curl", "-s",
            f"{HOST}/api/2.0/sql/statements/{sid}",
            "-H", f"Authorization: Bearer {TOKEN}"
        ]
        rp = subprocess.run(cmd_poll, capture_output=True, text=True, encoding="utf-8", errors="replace")
        try:
            d = json.loads(rp.stdout)
        except json.JSONDecodeError:
            time.sleep(5)
            continue

        st = d.get("status", {}).get("state", "?")
        if st == "SUCCEEDED":
            return _extract_status(d)
        elif st == "FAILED":
            err = d.get("status", {}).get("error", {}).get("message", "?")
            print(f"  FAILED: {err}")
            return None
        time.sleep(5)

    print(f"  TIMEOUT after {timeout}s")
    return None


def _extract_status(d):
    """Return success indicator."""
    rows = d.get("result", {}).get("data_array", [])
    print(f"  OK ({len(rows)} rows returned)")
    return True


def wait_for_warehouse():
    print("Starting warehouse...")
    cmd = [
        "curl", "-s", "-X", "POST",
        f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}/start",
        "-H", f"Authorization: Bearer {TOKEN}"
    ]
    subprocess.run(cmd, capture_output=True, text=True)

    for i in range(60):
        cmd_check = [
            "curl", "-s",
            f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}",
            "-H", f"Authorization: Bearer {TOKEN}"
        ]
        r = subprocess.run(cmd_check, capture_output=True, text=True, encoding="utf-8", errors="replace")
        try:
            state = json.loads(r.stdout).get("state", "UNKNOWN")
        except json.JSONDecodeError:
            state = "UNKNOWN"
        if state == "RUNNING":
            print("Warehouse is RUNNING")
            return True
        print(f"  Warehouse state: {state} (attempt {i+1}/60)")
        time.sleep(10)
    print("ERROR: Warehouse did not start in time")
    return False


# ============================================================================
# TABLE BUILDERS
# ============================================================================

def create_schema():
    run_sql("Create schema if not exists",
            f"CREATE SCHEMA IF NOT EXISTS {S} COMMENT 'Gold | MFAH Housing Needs Assessment for 7 Calgary-region CSDs'")


def create_geography_dim():
    sql = f"""CREATE OR REPLACE TABLE {S}.geography_dim
COMMENT 'Gold | 7 MFAH CSD communities with geocodes and coordinates. Source: Census 2021, HART HNA reports.'
AS SELECT * FROM VALUES
  ('CSD_4806021', 'Airdrie',            'CSD', 'airdrie',     51.2917, -114.0144),
  ('CSD_4806009', 'Cochrane',           'CSD', 'cochrane',    51.1892, -114.4700),
  ('CSD_4806006', 'Okotoks',            'CSD', 'okotoks',     50.7267, -113.9750),
  ('CSD_4806017', 'Chestermere',        'CSD', 'chestermere', 51.0350, -113.8231),
  ('CSD_4805018', 'Strathmore',         'CSD', 'strathmore',  51.0378, -113.4003),
  ('CSD_4806014', 'Rocky View County',  'CSD', 'rockyview',   51.2000, -114.3000),
  ('CSD_4806024', 'Crossfield',         'CSD', 'crossfield',  51.4342, -113.9678)
AS t(geography_id, geography, geography_type, geography_key, latitude, longitude)"""
    run_sql("Table 1/20: geography_dim", sql)


def create_timeframe_dim():
    rows = []
    for y in range(2006, 2026):
        tt = 'census_year' if y in (2006, 2011, 2016, 2021) else 'annual'
        ic = 'true' if y in (2006, 2011, 2016, 2021) else 'false'
        rows.append(f"  ({y}, '{y}', '{tt}', {ic})")
    sql = f"""CREATE OR REPLACE TABLE {S}.timeframe_dim
COMMENT 'Gold | Reference years spanning 2006-2025 for all time-series fact tables. Source: derived.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(timeframe_id, timeframe, timeframe_type, is_census_year)"""
    run_sql("Table 2/20: timeframe_dim", sql)


def create_source_dim():
    sql = f"""CREATE OR REPLACE TABLE {S}.source_dim
COMMENT 'Gold | Data provenance registry for all HNA sources. Source: HNA Full Validation CSV.'
AS SELECT * FROM VALUES
  ('census_2021',     'Census of Population 2021',                'Statistics Canada', 'National census microdata for population, housing, income',     'quinquennial', 'https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/index-eng.cfm'),
  ('census_2016',     'Census of Population 2016',                'Statistics Canada', 'National census microdata for population, housing, income',     'quinquennial', 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/index-eng.cfm'),
  ('census_2011',     'Census of Population 2011',                'Statistics Canada', 'National census microdata for population, housing, income',     'quinquennial', 'https://www12.statcan.gc.ca/census-recensement/2011/dp-pd/index-eng.cfm'),
  ('census_2006',     'Census of Population 2006',                'Statistics Canada', 'National census microdata for population, housing, income',     'quinquennial', 'https://www12.statcan.gc.ca/census-recensement/2006/dp-pd/index-eng.cfm'),
  ('hart_hna_2021',   'HART Housing Needs Assessment 2021',       'HART',             'Housing needs assessment including deficit, income, priority',  'one-time',     'https://hart.ubc.ca/'),
  ('cmhc_rms',        'CMHC Rental Market Survey',                'CMHC',             'Primary rental market vacancy and rent data',                  'annual',       'https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market'),
  ('cmhc_starts',     'CMHC Housing Starts',                      'CMHC',             'New housing construction starts by structure type and tenure',  'monthly',      'https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/housing-starts-completions'),
  ('esdc_ei',         'EI Recipients by CSD',                     'ESDC',             'Employment Insurance regular beneficiaries by geography',       'annual',       'https://open.canada.ca/data/en/dataset/employment-insurance'),
  ('alberta_permits', 'Alberta Regional Dashboard Building Permits','Government of Alberta','Building permit counts and values by sector',             'annual',       'https://regionaldashboard.alberta.ca/'),
  ('census_chn',      'Census Core Housing Need (Table 98-10-0248)','Statistics Canada','Core housing need by tenure, adequacy, suitability, affordability','quinquennial','https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=9810024801'),
  ('cmhc_rental_2024','CMHC Rental Market October 2024',          'CMHC',             'Rent by bedroom type from October 2024 rental market survey',  'annual',       'https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market'),
  ('creb_mls',        'CREB MLS Data',                            'CREB',             'Calgary Real Estate Board MLS residential sales data',         'monthly',      'https://www.creb.com/Housing_Statistics/')
AS t(source_id, source_name, source_organization, source_description, refresh_cadence, reference_url)"""
    run_sql("Table 3/20: source_dim", sql)


def _geo(key):
    """Return geography_id for a community key."""
    m = {
        'airdrie': 'CSD_4806021', 'cochrane': 'CSD_4806009', 'okotoks': 'CSD_4806006',
        'chestermere': 'CSD_4806017', 'strathmore': 'CSD_4805018', 'rockyview': 'CSD_4806014',
        'crossfield': 'CSD_4806024'
    }
    return m[key]


def create_hna_demographics_csd_2021():
    # Data from communityData: pop2016, pop2021, growth5yr, growth10yr, households, dwellings, singleDetached, tenantPct, medianIncome, medianRent, ageDistribution
    data = {
        'airdrie':     (61190, 73795, 20.6, 73.5, 26295, 26295, 17215, 20.7, 110000, 1650, 24.7, 10.7, 33.0, 22.6, 9.0),
        'cochrane':    (25640, 31470, 22.7, 54.0, 12100, 12100, 7225,  16.3, 113000, 1292, 21.7, 9.5, 29.2, 25.2, 14.4),
        'okotoks':     (28881, 30405, 5.3,  22.0, 11200, 11200, 7200,  16.2, 118000, 1450, 18.2, 11.8, 25.8, 27.5, 16.7),
        'chestermere': (19887, 21952, 10.4, 35.0, 7500,  7500,  5800,  14.8, 135000, 1700, 21.5, 12.2, 29.5, 26.0, 10.8),
        'strathmore':  (13756, 14339, 4.2,  17.0, 5400,  5400,  3600,  22.5, 95000,  1250, 17.5, 12.5, 24.8, 26.2, 19.0),
        'rockyview':   (39407, 42635, 8.2,  28.0, 14200, 14200, 12500, 8.2,  142000, 1400, 16.8, 10.5, 22.5, 30.2, 20.0),
        'crossfield':  (3100,  3450,  11.3, 32.0, 1250,  1250,  950,   19.5, 88000,  1150, 18.5, 13.2, 26.5, 25.8, 16.0),
    }
    rows = []
    for k, v in data.items():
        gid = _geo(k)
        rows.append(f"  ('{gid}', {v[0]}, {v[1]}, {v[2]}, {v[3]}, {v[4]}, {v[5]}, {v[6]}, {v[7]}, {v[8]}, {v[9]}, {v[10]}, {v[11]}, {v[12]}, {v[13]}, {v[14]}, 'census_2021')")
    sql = f"""CREATE OR REPLACE TABLE {S}.hna_demographics_csd_2021
COMMENT 'Gold | Population, household, income and age demographics for 7 CSDs. Source: Census 2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, pop_2016, pop_2021, growth_5yr_pct, growth_10yr_pct, households, dwellings, single_detached, tenant_pct, median_income, median_rent, age_0_14_pct, age_15_24_pct, age_25_44_pct, age_45_64_pct, age_65plus_pct, source_id)"""
    run_sql("Table 4/20: hna_demographics_csd_2021", sql)


def create_hna_housing_indicators_csd_2021():
    # From housingIndicators{} in each community
    data = {
        'airdrie':     (26295, 20850, 5445, 565, 420, 140, 2.1, 2.0, 2.6, 970, 565, 400, 3.7, 2.7, 7.3, 5450, 3515, 1940, 20.8, 16.9, 35.7, 1990, 955, 1030, 7.7, 4.6, 19.6),
        'cochrane':    (12100, 10125, 1970, 270, 200, 70,  2.2, 2.0, 3.6, 265, 175, 90,  2.2, 1.7, 4.6, 2265, 1445, 820,  18.8, 14.3, 42.1, 800,  360, 440,  6.8, 3.6, 23.4),
        'okotoks':     (11200, 9382,  1818, 560, 405, 155, 5.0, 4.3, 8.5, 336, 168, 168, 3.0, 1.8, 9.2, 1904, 1125, 779,  17.0, 12.0, 42.9, 560,  300, 260,  5.0, 3.2, 14.3),
        'chestermere': (7500,  6388,  1112, 338, 255, 83,  4.5, 4.0, 7.5, 225, 115, 110, 3.0, 1.8, 9.9, 1200, 735,  465,  16.0, 11.5, 41.8, 375,  195, 180,  5.0, 3.1, 16.2),
        'strathmore':  (5400,  4185,  1215, 324, 230, 94,  6.0, 5.5, 7.7, 216, 96,  120, 4.0, 2.3, 9.9, 1188, 628,  560,  22.0, 15.0, 46.1, 432,  200, 232,  8.0, 4.8, 19.1),
        'rockyview':   (14200, 13035, 1165, 710, 585, 125, 5.0, 4.5, 10.7, 284, 182, 102, 2.0, 1.4, 8.8, 1846, 1435, 411,  13.0, 11.0, 35.3, 568,  385, 183,  4.0, 3.0, 15.7),
        'crossfield':  (1250,  1006,  244,  88,  63,  25,  7.0, 6.3, 10.2, 63,  28,  35,  5.0, 2.8, 14.3, 300,  151,  149,  24.0, 15.0, 61.1, 106,  48,  58,   8.5, 4.8, 23.8),
    }
    rows = []
    for k, v in data.items():
        gid = _geo(k)
        rows.append(f"  ('{gid}', {v[0]}, {v[1]}, {v[2]}, {v[3]}, {v[4]}, {v[5]}, {v[6]}, {v[7]}, {v[8]}, {v[9]}, {v[10]}, {v[11]}, {v[12]}, {v[13]}, {v[14]}, {v[15]}, {v[16]}, {v[17]}, {v[18]}, {v[19]}, {v[20]}, {v[21]}, {v[22]}, {v[23]}, {v[24]}, {v[25]}, {v[26]}, 'census_chn')")
    sql = f"""CREATE OR REPLACE TABLE {S}.hna_housing_indicators_csd_2021
COMMENT 'Gold | Core housing need, adequacy, suitability, affordability by tenure for 7 CSDs. Source: Census Table 98-10-0248.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, total_households, owner_total, renter_total, inadequate_count, inadequate_owner, inadequate_renter, inadequate_pct, inadequate_owner_pct, inadequate_renter_pct, unsuitable_count, unsuitable_owner, unsuitable_renter, unsuitable_pct, unsuitable_owner_pct, unsuitable_renter_pct, unaffordable_count, unaffordable_owner, unaffordable_renter, unaffordable_pct, unaffordable_owner_pct, unaffordable_renter_pct, chn_total, chn_owner, chn_renter, chn_pct, chn_owner_pct, chn_renter_pct, source_id)"""
    run_sql("Table 5/20: hna_housing_indicators_csd_2021", sql)


def create_hna_income_categories_csd_2021():
    # 5 tiers per community = 35 rows
    # Airdrie & Cochrane have maxIncome/maxShelter; others use shelterVeryLow etc.
    tiers = ['very_low', 'low', 'moderate', 'median', 'high']
    data = {
        'airdrie': [
            (1.28, 337, 22000, 550),
            (14.44, 3797, 55000, 1375),
            (20.27, 5332, 88000, 2200),
            (26.52, 6973, 132000, 3300),
            (37.5, 9861, 132001, 3301),
        ],
        'cochrane': [
            (2.03, 246, 22600, 565),
            (14.64, 1772, 56500, 1413),
            (20.59, 2491, 90400, 2260),
            (24.43, 2956, 135600, 3390),
            (38.31, 4635, 135601, 3391),
        ],
        'okotoks': [
            (1.1, 125, None, 590),
            (12.5, 1400, None, 1475),
            (19.2, 2150, None, 2360),
            (27.8, 3115, None, 3540),
            (39.4, 4410, None, 4425),
        ],
        'chestermere': [
            (0.7, 55, None, 675),
            (9.8, 735, None, 1688),
            (15.6, 1170, None, 2700),
            (29.2, 2190, None, 4050),
            (44.7, 3350, None, 5063),
        ],
        'strathmore': [
            (1.8, 95, None, 475),
            (16.2, 875, None, 1188),
            (22.5, 1215, None, 1900),
            (25.8, 1395, None, 2850),
            (33.7, 1820, None, 3563),
        ],
        'rockyview': [
            (0.6, 85, None, 710),
            (8.5, 1205, None, 1775),
            (14.2, 2015, None, 2840),
            (26.8, 3805, None, 4260),
            (49.9, 7090, None, 5325),
        ],
        'crossfield': [
            (2.0, 25, None, 440),
            (17.5, 220, None, 1100),
            (24.2, 300, None, 1760),
            (26.5, 330, None, 2640),
            (29.8, 375, None, 3300),
        ],
    }
    rows = []
    for k, tier_data in data.items():
        gid = _geo(k)
        for i, (pct, count, max_inc, max_shelter) in enumerate(tier_data):
            mi = str(max_inc) if max_inc is not None else 'NULL'
            rows.append(f"  ('{gid}', '{tiers[i]}', {pct}, {count}, CAST({mi} AS INT), {max_shelter}, 'hart_hna_2021')")
    sql = f"""CREATE OR REPLACE TABLE {S}.hna_income_categories_csd_2021
COMMENT 'Gold | HART income category distribution (5 tiers) for 7 CSDs. Source: HART HNA 2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, income_tier, household_pct, household_count, max_income, max_shelter_cost, source_id)"""
    run_sql("Table 6/20: hna_income_categories_csd_2021", sql)


def create_hna_housing_deficit_csd_2021():
    tiers = ['very_low', 'low', 'moderate', 'median', 'high']
    sizes = ['p1', 'p2', 'p3', 'p4', 'p5']

    # Full breakdown for Airdrie and Cochrane
    full_data = {
        'airdrie': {
            'very_low': {'p1': 225, 'p2': 45, 'p3': 0, 'p4': 0, 'p5': 0, 'total': 270},
            'low':      {'p1': 580, 'p2': 480, 'p3': 245, 'p4': 160, 'p5': 85, 'total': 1550},
            'moderate': {'p1': 0, 'p2': 0, 'p3': 0, 'p4': 40, 'p5': 95, 'total': 135},
            'median':   {'p1': 0, 'p2': 0, 'p3': 0, 'p4': 0, 'p5': 0, 'total': 0},
            'high':     {'p1': 0, 'p2': 0, 'p3': 0, 'p4': 0, 'p5': 0, 'total': 0},
        },
        'cochrane': {
            'very_low': {'p1': 160, 'p2': 25, 'p3': 0, 'p4': 0, 'p5': 0, 'total': 185},
            'low':      {'p1': 210, 'p2': 185, 'p3': 100, 'p4': 80, 'p5': 15, 'total': 590},
            'moderate': {'p1': 0, 'p2': 0, 'p3': 0, 'p4': 0, 'p5': 0, 'total': 0},
            'median':   {'p1': 0, 'p2': 0, 'p3': 0, 'p4': 0, 'p5': 0, 'total': 0},
            'high':     {'p1': 0, 'p2': 0, 'p3': 0, 'p4': 0, 'p5': 0, 'total': 0},
        },
    }

    # Totals only for remaining 5
    totals_only = {
        'okotoks':     {'very_low': 125, 'low': 295, 'moderate': 0, 'median': 0, 'high': 0},
        'chestermere': {'very_low': 55, 'low': 225, 'moderate': 0, 'median': 0, 'high': 0},
        'strathmore':  {'very_low': 95, 'low': 285, 'moderate': 0, 'median': 0, 'high': 0},
        'rockyview':   {'very_low': 85, 'low': 235, 'moderate': 0, 'median': 0, 'high': 0},
        'crossfield':  {'very_low': 25, 'low': 60, 'moderate': 0, 'median': 0, 'high': 0},
    }

    rows = []
    # Full breakdown communities
    for k in ['airdrie', 'cochrane']:
        gid = _geo(k)
        for tier in tiers:
            td = full_data[k][tier]
            for sz in sizes:
                rows.append(f"  ('{gid}', '{tier}', '{sz}', {td[sz]}, {td['total']}, 'hart_hna_2021')")

    # Totals-only communities
    for k in ['okotoks', 'chestermere', 'strathmore', 'rockyview', 'crossfield']:
        gid = _geo(k)
        for tier in tiers:
            total = totals_only[k][tier]
            for sz in sizes:
                rows.append(f"  ('{gid}', '{tier}', '{sz}', NULL, {total}, 'hart_hna_2021')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_housing_deficit_csd_2021
COMMENT 'Gold | Affordable housing deficit by income tier and household size for 7 CSDs. Source: HART HNA 2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, income_tier, household_size, size_deficit, tier_total_deficit, source_id)"""
    run_sql("Table 7/20: hna_housing_deficit_csd_2021", sql)


def create_hna_rental_supply_csd_2021():
    data = {
        'airdrie':     (1543, 3892, 95, 5340, 5435, 1.75, 4870, 91.3, 0, 225, -510, 735),
        'cochrane':    (212,  1738, 95, 1855, 1950, 4.87, 1715, 92.5, 0, 65,  -180, 245),
        'okotoks':     (520,  1298, 38, 1780, 1818, 2.09, 1580, 86.9, 0, 78,  -145, 223),
        'chestermere': (285,  827,  22, 1090, 1112, 1.98, 920,  82.7, 0, 45,  -95,  140),
        'strathmore':  (380,  835,  65, 1150, 1215, 5.35, 1050, 86.4, 0, 55,  -85,  140),
        'rockyview':   (245,  920,  125, 1040, 1165, 10.7, 980,  84.1, 0, 65,  -110, 175),
        'crossfield':  (58,   186,  8,  236,  244,  3.28, 205,  84.0, 0, 12,  -25,  37),
    }
    rows = []
    for k, v in data.items():
        gid = _geo(k)
        rows.append(f"  ('{gid}', {v[0]}, {v[1]}, {v[2]}, {v[3]}, {v[4]}, {v[5]}, {v[6]}, {v[7]}, {v[8]}, {v[9]}, {v[10]}, {v[11]}, 'hart_hna_2021')")
    sql = f"""CREATE OR REPLACE TABLE {S}.hna_rental_supply_csd_2021
COMMENT 'Gold | Rental supply composition including subsidized, below-market and affordable changes for 7 CSDs. Source: HART HNA 2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, primary_rental, secondary_rental, subsidized, unsubsidized, total_rental, subsidized_pct, below_market, below_market_pct, co_op_units, affordable_built_2016_2021, affordable_lost_2016_2021, affordable_net_change, source_id)"""
    run_sql("Table 8/20: hna_rental_supply_csd_2021", sql)


def create_hna_priority_groups_csd_2021():
    # Full priority group data for Airdrie and Cochrane
    groups_full = {
        'airdrie': {
            'youth_18_29': (200, 9.5), 'transgender': (0, 0.0),
            'mental_health_addictions': (245, 6.7), 'veterans': (40, 3.7),
            'single_mothers': (405, 19.7), 'women_led': (1180, 11.3),
            'indigenous': (170, 9.6), 'visible_minority': (365, 7.1),
            'black_led': (85, 9.6), 'new_migrants': (70, 10.1),
            'refugees': (65, 11.0), 'hh_under_25': (75, 14.9),
            'hh_over_65': (555, 14.8), 'hh_over_85': (55, 23.4),
            'physical_limitation': (560, 7.5), 'mental_health': (395, 7.1),
            'same_gender': (0, 0.0), 'community_total': (1990, 7.7),
        },
        'cochrane': {
            'youth_18_29': (55, 7.9), 'transgender': (0, 0.0),
            'mental_health_addictions': (65, 4.1), 'veterans': (10, 2.2),
            'single_mothers': (185, 26.6), 'women_led': (500, 10.5),
            'indigenous': (65, 9.6), 'visible_minority': (75, 6.6),
            'black_led': (15, 20.0), 'new_migrants': (0, 0.0),
            'refugees': (15, 20.0), 'hh_under_25': (20, 13.8),
            'hh_over_65': (260, 9.5), 'hh_over_85': (45, 22.0),
            'physical_limitation': (165, 5.1), 'mental_health': (150, 6.1),
            'same_gender': (0, 0.0), 'community_total': (800, 6.8),
        },
    }

    # Other 5 communities: community_total only (from housingIndicators chnTotal/chnPct)
    other_totals = {
        'okotoks':     (560, 5.0),
        'chestermere': (375, 5.0),
        'strathmore':  (432, 8.0),
        'rockyview':   (568, 4.0),
        'crossfield':  (106, 8.5),
    }

    rows = []
    for k in ['airdrie', 'cochrane']:
        gid = _geo(k)
        for grp, (cnt, rate) in groups_full[k].items():
            rows.append(f"  ('{gid}', '{grp}', {cnt}, {rate}, 'hart_hna_2021')")

    for k, (cnt, rate) in other_totals.items():
        gid = _geo(k)
        rows.append(f"  ('{gid}', 'community_total', {cnt}, {rate}, 'hart_hna_2021')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_priority_groups_csd_2021
COMMENT 'Gold | Core housing need by priority population group for 7 CSDs. Source: HART HNA 2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, priority_group, chn_count, chn_pct, source_id)"""
    run_sql("Table 9/20: hna_priority_groups_csd_2021", sql)


def create_hna_economic_snapshot_csd_2021():
    # unemploymentRate2021, lowIncomeLIM{}, avgSalePrice, ownerIncome, renterIncome, netCommuterFlow*
    data = {
        'airdrie':     (11.1, 6.0, 7.2, 5.2, 7.8, 418400, 121000, 74500, -8210, -11700, -8310, 29.0),
        'cochrane':    (9.5,  6.0, 7.0, 5.4, 7.1, 466800, 122000, 66500, -2485, -3655, -2445, 33.1),
        'okotoks':     (6.5,  6.2, 7.0, 5.8, 7.2, 520000, 128000, 78000, None, -5100, -3920, 23.1),
        'chestermere': (5.8,  5.5, 6.2, 5.0, 6.5, 620000, 145000, 88000, None, -4800, -3650, 24.0),
        'strathmore':  (9.2,  9.5, 11.2, 8.8, 10.5, 420000, 105000, 68000, None, -2400, -1850, 22.9),
        'rockyview':   (5.5,  5.0, 5.5, 4.5, 6.2, 680000, 155000, 92000, None, -5800, -4200, 27.6),
        'crossfield':  (9.8,  10.2, 12.0, 9.5, 11.0, 380000, 98000, 62000, None, -720, -540, 25.0),
    }
    rows = []
    for k, v in data.items():
        gid = _geo(k)
        ncf2011 = str(v[8]) if v[8] is not None else 'NULL'
        rows.append(f"  ('{gid}', {v[0]}, {v[1]}, {v[2]}, {v[3]}, {v[4]}, {v[5]}, {v[6]}, {v[7]}, CAST({ncf2011} AS INT), {v[9]}, {v[10]}, {v[11]}, 'census_2021')")
    sql = f"""CREATE OR REPLACE TABLE {S}.hna_economic_snapshot_csd_2021
COMMENT 'Gold | Economic indicators including unemployment, low income, sale prices, commuting for 7 CSDs. Source: Census 2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, unemployment_rate_2021, lim_all_pop_pct, lim_0_17_pct, lim_18_64_pct, lim_65plus_pct, avg_sale_price, owner_income, renter_income, net_commuter_flow_2011, net_commuter_flow_2016, net_commuter_flow_2021, net_commuter_flow_change_pct, source_id)"""
    run_sql("Table 10/20: hna_economic_snapshot_csd_2021", sql)


def create_hna_structure_type_csd_2006_2021():
    # Only Airdrie and Cochrane have structureType{}
    struct_data = {
        'airdrie': {
            2006: (7330, 630, 1005, 40, 1020, 0, 65, 10095),
            2011: (10800, 935, 1890, 60, 1125, 50, 160, 15025),
            2016: (14735, 1310, 2695, 40, 2610, 35, 235, 21645),
            2021: (17215, 1660, 3780, 75, 3270, 40, 255, 26295),
        },
        'cochrane': {
            2006: (3425, 735, 220, 35, 390, 0, 35, 4840),
            2011: (4505, 945, 380, 55, 635, 0, 0, 6525),
            2016: (6140, 1360, 1140, 100, 1010, 0, 0, 9755),
            2021: (7225, 1740, 1780, 110, 1215, 0, 145, 12095),
        },
    }
    types = ['single_detached', 'semi_detached', 'row', 'duplex', 'low_rise', 'high_rise', 'other']
    census_src = {2006: 'census_2006', 2011: 'census_2011', 2016: 'census_2016', 2021: 'census_2021'}

    rows = []
    for k, years in struct_data.items():
        gid = _geo(k)
        for yr, vals in years.items():
            total = vals[7]
            for i, stype in enumerate(types):
                rows.append(f"  ('{gid}', {yr}, '{stype}', {vals[i]}, {total}, '{census_src[yr]}')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_structure_type_csd_2006_2021
COMMENT 'Gold | Dwelling structure type distribution by census year for Airdrie and Cochrane. Source: Census 2006-2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, structure_type, unit_count, year_total, source_id)"""
    run_sql("Table 11/20: hna_structure_type_csd_2006_2021", sql)


def create_hna_shelter_cost_csd_2006_2021():
    # Only Airdrie and Cochrane have shelterHist{}
    shelter_data = {
        'airdrie': {
            2006: (1290, 1825, 4235, 2085, 655, 10090),
            2011: (1545, 1825, 3185, 4870, 3600, 15025),
            2016: (1475, 2485, 3785, 6190, 7700, 21635),
            2021: (1020, 3480, 4335, 6310, 11150, 26295),
        },
        'cochrane': {
            2006: (760, 1205, 1380, 1030, 460, 4830),
            2011: (715, 1475, 1295, 1560, 1480, 6520),
            2016: (665, 1955, 1380, 2235, 3485, 9725),
            2021: (565, 2675, 1535, 2675, 4635, 12095),
        },
    }
    bands = ['under_500', 'r500_999', 'r1000_1499', 'r1500_1999', 'r2000_plus']
    census_src = {2006: 'census_2006', 2011: 'census_2011', 2016: 'census_2016', 2021: 'census_2021'}

    rows = []
    for k, years in shelter_data.items():
        gid = _geo(k)
        for yr, vals in years.items():
            total = vals[5]
            for i, band in enumerate(bands):
                rows.append(f"  ('{gid}', {yr}, '{band}', {vals[i]}, {total}, '{census_src[yr]}')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_shelter_cost_csd_2006_2021
COMMENT 'Gold | Shelter cost distribution by band and census year for Airdrie and Cochrane. Source: Census 2006-2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, cost_band, household_count, year_total, source_id)"""
    run_sql("Table 12/20: hna_shelter_cost_csd_2006_2021", sql)


def create_hna_income_distribution_csd_2006_2021():
    # Only Airdrie and Cochrane have incomeDistTotal/Owner/Renter{}
    inc_dist = {
        'airdrie': {
            'total': {
                2006: (525, 1250, 1600, 1860, 1725, 3135, 10095),
                2011: (860, 1195, 1520, 2045, 2095, 7310, 15025),
                2016: (625, 1460, 2100, 2485, 2735, 12240, 21645),
                2021: (515, 1910, 2670, 3195, 3280, 14720, 26295),
            },
            'owner': {
                2006: (355, 915, 1370, 1685, 1630, 3065, 9025),
                2011: (670, 885, 1195, 1630, 1920, 6840, 13135),
                2016: (375, 930, 1460, 1860, 2210, 11040, 17870),
                2021: (310, 1065, 1735, 2180, 2500, 13060, 20850),
            },
            'renter': {
                2006: (170, 340, 235, 170, 100, 65, 1070),
                2011: (190, 310, 330, 410, 180, 470, 1890),
                2016: (250, 535, 640, 620, 530, 1200, 3775),
                2021: (210, 850, 940, 1015, 775, 1665, 5445),
            },
        },
        'cochrane': {
            'total': {
                2006: (350, 545, 675, 730, 585, 1950, 4840),
                2011: (405, 530, 810, 685, 875, 3220, 6525),
                2016: (295, 680, 955, 1050, 1095, 5690, 9755),
                2021: (320, 910, 1165, 1285, 1565, 6850, 12095),
            },
            'owner': {
                2006: (185, 340, 535, 670, 515, 1885, 4130),
                2011: (300, 340, 615, 570, 785, 3065, 5665),
                2016: (145, 455, 715, 840, 905, 5320, 8380),
                2021: (185, 535, 785, 1010, 1300, 6315, 10125),
            },
            'renter': {
                2006: (170, 210, 140, 60, 65, 70, 710),
                2011: (110, 185, 195, 115, 100, 155, 860),
                2016: (150, 220, 240, 210, 185, 370, 1380),
                2021: (140, 370, 380, 275, 265, 535, 1970),
            },
        },
    }
    income_bands = ['under_20k', 'r20k_40k', 'r40k_60k', 'r60k_80k', 'r80k_100k', 'over_100k']
    census_src = {2006: 'census_2006', 2011: 'census_2011', 2016: 'census_2016', 2021: 'census_2021'}

    rows = []
    for k, tenures in inc_dist.items():
        gid = _geo(k)
        for tenure, years in tenures.items():
            for yr, vals in years.items():
                total = vals[6]
                for i, band in enumerate(income_bands):
                    rows.append(f"  ('{gid}', {yr}, '{tenure}', '{band}', {vals[i]}, {total}, '{census_src[yr]}')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_income_distribution_csd_2006_2021
COMMENT 'Gold | Household income distribution by band, tenure and census year for Airdrie and Cochrane. Source: Census 2006-2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, tenure_type, income_band, household_count, year_total, source_id)"""
    run_sql("Table 13/20: hna_income_distribution_csd_2006_2021", sql)


def create_hna_dwelling_values_csd_2006_2021():
    # Only Airdrie and Cochrane have avgDwellingValue{}
    dv_data = {
        'airdrie': {
            2006: (297232, 243510, 211234, 880726, 190905, 0, 41431, 279053),
            2011: (395428, 308137, 263296, 0, 230204, 0, 136355, 364212),
            2016: (459892, 345868, 312809, 0, 222567, 0, 215411, 418413),
            2021: (460400, 363200, 304000, 520000, 205600, 0, 210000, 418400),
        },
        'cochrane': {
            2006: (394791, 311608, 229655, 0, 227899, 0, 44138, 370160),
            2011: (475256, 332268, 311027, 328294, 235695, 0, 0, 432439),
            2016: (517837, 399678, 339192, 475604, 263650, 0, 0, 465460),
            2021: (517500, 416000, 354000, 420000, 256000, 0, 112000, 466800),
        },
    }
    types = ['single_detached', 'semi_detached', 'row', 'duplex', 'low_rise', 'high_rise', 'other']
    census_src = {2006: 'census_2006', 2011: 'census_2011', 2016: 'census_2016', 2021: 'census_2021'}

    rows = []
    for k, years in dv_data.items():
        gid = _geo(k)
        for yr, vals in years.items():
            total_val = vals[7]
            for i, stype in enumerate(types):
                rows.append(f"  ('{gid}', {yr}, '{stype}', {vals[i]}, {total_val}, '{census_src[yr]}')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_dwelling_values_csd_2006_2021
COMMENT 'Gold | Average dwelling value by structure type and census year for Airdrie and Cochrane. Source: Census 2006-2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, structure_type, avg_value, year_avg_total, source_id)"""
    run_sql("Table 14/20: hna_dwelling_values_csd_2006_2021", sql)


def create_hna_median_income_csd_2006_2021():
    # Only Airdrie and Cochrane have incomeMedianTotal/Owner/Renter and incomeAvgTotal/Owner/Renter
    income_ts = {
        'airdrie': {
            2006: (78097, 81644, 41361, 85643, 90022, 48772),
            2011: (97818, 102584, 64839, 106616, 111629, 71769),
            2016: (110405, 118683, 74510, 124450, 133146, 83328),
            2021: (110000, 121000, 74500, 122700, 132600, 85000),
        },
        'cochrane': {
            2006: (83003, 92260, 37996, 103823, 112868, 51031),
            2011: (97923, 104109, 51906, 110779, 117631, 65557),
            2016: (113230, 120826, 66141, 137234, 147076, 77446),
            2021: (113000, 122000, 66500, 130800, 140800, 79500),
        },
    }
    census_src = {2006: 'census_2006', 2011: 'census_2011', 2016: 'census_2016', 2021: 'census_2021'}

    rows = []
    for k, years in income_ts.items():
        gid = _geo(k)
        for yr, vals in years.items():
            rows.append(f"  ('{gid}', {yr}, {vals[0]}, {vals[1]}, {vals[2]}, {vals[3]}, {vals[4]}, {vals[5]}, '{census_src[yr]}')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_median_income_csd_2006_2021
COMMENT 'Gold | Median and average household income by tenure and census year for Airdrie and Cochrane. Source: Census 2006-2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, median_total, median_owner, median_renter, avg_total, avg_owner, avg_renter, source_id)"""
    run_sql("Table 15/20: hna_median_income_csd_2006_2021", sql)


def create_cmhc_rental_market_csd_2016_2025():
    # All 7 communities have vacancyHistory + rentHistory (2016-2023)
    # Airdrie+Cochrane also have rent by bedroom and rent3Bed trends
    vacancy = {
        'airdrie':     {2016: 12.5, 2017: 7.6, 2018: 3.8, 2019: 7.0, 2020: 6.6, 2021: 2.6, 2022: 1.6, 2023: 1.1},
        'cochrane':    {2016: 3.7, 2017: 3.2, 2018: 4.0, 2019: 2.4, 2020: 0.0, 2021: 1.2, 2022: 0.5, 2023: 0.5},
        'okotoks':     {2016: 6.8, 2017: 4.5, 2018: 2.8, 2019: 3.5, 2020: 3.2, 2021: 2.2, 2022: 1.8, 2023: 2.9},
        'chestermere': {2016: 9.5, 2017: 6.2, 2018: 3.8, 2019: 5.2, 2020: 4.8, 2021: 3.2, 2022: 2.5, 2023: 3.8},
        'strathmore':  {2016: 10.2, 2017: 7.5, 2018: 5.2, 2019: 6.8, 2020: 6.2, 2021: 4.5, 2022: 3.2, 2023: 4.8},
        'rockyview':   {2016: 12.5, 2017: 9.8, 2018: 7.2, 2019: 8.5, 2020: 7.8, 2021: 6.2, 2022: 5.5, 2023: 7.2},
        'crossfield':  {2016: 9.8, 2017: 7.2, 2018: 5.0, 2019: 6.5, 2020: 5.8, 2021: 4.2, 2022: 3.5, 2023: 4.8},
    }
    avg_rent = {
        'airdrie':     {2016: 1218, 2017: 1228, 2018: 1253, 2019: 1284, 2020: 1258, 2021: 1289, 2022: 1342, 2023: 1485},
        'cochrane':    {2016: 1087, 2017: 1072, 2018: 1104, 2019: 1072, 2020: 1148, 2021: 1217, 2022: 1292, 2023: 1405},
        'okotoks':     {2016: 1180, 2017: 1210, 2018: 1265, 2019: 1320, 2020: 1305, 2021: 1380, 2022: 1485, 2023: 1625},
        'chestermere': {2016: 1420, 2017: 1455, 2018: 1510, 2019: 1580, 2020: 1565, 2021: 1620, 2022: 1745, 2023: 1920},
        'strathmore':  {2016: 985, 2017: 1005, 2018: 1045, 2019: 1095, 2020: 1080, 2021: 1125, 2022: 1210, 2023: 1350},
        'rockyview':   {2016: 1120, 2017: 1145, 2018: 1195, 2019: 1255, 2020: 1240, 2021: 1295, 2022: 1395, 2023: 1545},
        'crossfield':  {2016: 885, 2017: 905, 2018: 945, 2019: 995, 2020: 980, 2021: 1025, 2022: 1105, 2023: 1235},
    }
    # Bedroom rents: Airdrie and Cochrane have rent3Bed2021-2025, rentBachelor/1Bed/2Bed/3Bed (Oct 2024)
    # Others have rentBachelor/1Bed/2Bed/3Bed and rent3Bed2021-2025 too
    bedroom_rents = {
        'airdrie':     {'bachelor': 0, 'r1bed': 1380, 'r2bed': 1760, 'r3bed': 2595},
        'cochrane':    {'bachelor': 0, 'r1bed': 0, 'r2bed': 0, 'r3bed': 2295},
        'okotoks':     {'bachelor': 925, 'r1bed': 1320, 'r2bed': 1520, 'r3bed': 2280},
        'chestermere': {'bachelor': 1100, 'r1bed': 1480, 'r2bed': 1750, 'r3bed': 2620},
        'strathmore':  {'bachelor': 850, 'r1bed': 1150, 'r2bed': 1380, 'r3bed': 1950},
        'rockyview':   {'bachelor': 900, 'r1bed': 1250, 'r2bed': 1450, 'r3bed': 2100},
        'crossfield':  {'bachelor': 780, 'r1bed': 1050, 'r2bed': 1280, 'r3bed': 1750},
    }
    rent3bed_ts = {
        'airdrie':     {2021: 1445, 2022: 1895, 2023: 1895, 2024: 2595, 2025: 2675},
        'cochrane':    {2021: 1475, 2022: 1650, 2023: 2515, 2024: 2295, 2025: 2080},
        'okotoks':     {2021: 1380, 2022: 1720, 2023: 1950, 2024: 2150, 2025: 2280},
        'chestermere': {2021: 1580, 2022: 1920, 2023: 2200, 2024: 2480, 2025: 2620},
        'strathmore':  {2021: 1180, 2022: 1450, 2023: 1680, 2024: 1850, 2025: 1950},
        'rockyview':   {2021: 1280, 2022: 1580, 2023: 1820, 2024: 1980, 2025: 2100},
        'crossfield':  {2021: 1050, 2022: 1320, 2023: 1520, 2024: 1680, 2025: 1750},
    }
    rent_change = {
        'airdrie':     {2017: 10, 2018: 25, 2019: 31, 2020: -26, 2021: 31, 2022: 53, 2023: 143},
        'cochrane':    {2017: -15, 2018: 32, 2019: -32, 2020: 76, 2021: 69, 2022: 75, 2023: 0},
        'okotoks':     {2017: 30, 2018: 55, 2019: 55, 2020: -15, 2021: 75, 2022: 105, 2023: 140},
        'chestermere': {2017: 35, 2018: 55, 2019: 70, 2020: -15, 2021: 55, 2022: 125, 2023: 175},
        'strathmore':  {2017: 20, 2018: 40, 2019: 50, 2020: -15, 2021: 45, 2022: 85, 2023: 140},
        'rockyview':   {2017: 25, 2018: 50, 2019: 60, 2020: -15, 2021: 55, 2022: 100, 2023: 150},
        'crossfield':  {2017: 20, 2018: 40, 2019: 50, 2020: -15, 2021: 45, 2022: 80, 2023: 130},
    }

    # Build one row per community per year (2016-2025)
    rows = []
    for k in ['airdrie', 'cochrane', 'okotoks', 'chestermere', 'strathmore', 'rockyview', 'crossfield']:
        gid = _geo(k)
        for yr in range(2016, 2026):
            vac = vacancy[k].get(yr)
            vac_s = str(vac) if vac is not None else 'NULL'
            ar = avg_rent[k].get(yr)
            ar_s = str(ar) if ar is not None else 'NULL'
            rc = rent_change[k].get(yr)
            rc_s = str(rc) if rc is not None else 'NULL'
            r3 = rent3bed_ts[k].get(yr)
            r3_s = str(r3) if r3 is not None else 'NULL'
            # Bedroom rents only for 2024 snapshot
            if yr == 2024:
                br = bedroom_rents[k]
                rb = str(br['bachelor']) if br['bachelor'] != 0 else 'NULL'
                r1 = str(br['r1bed']) if br['r1bed'] != 0 else 'NULL'
                r2 = str(br['r2bed']) if br['r2bed'] != 0 else 'NULL'
            else:
                rb = 'NULL'
                r1 = 'NULL'
                r2 = 'NULL'
            rows.append(f"  ('{gid}', {yr}, CAST({vac_s} AS DOUBLE), CAST({ar_s} AS INT), CAST({rc_s} AS INT), CAST({rb} AS INT), CAST({r1} AS INT), CAST({r2} AS INT), CAST({r3_s} AS INT), 'cmhc_rms')")

    sql = f"""CREATE OR REPLACE TABLE {S}.cmhc_rental_market_csd_2016_2025
COMMENT 'Gold | CMHC rental market vacancy rates, average rents and bedroom rents for 7 CSDs 2016-2025. Source: CMHC Rental Market Survey.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, vacancy_rate, avg_rent, rent_change_yoy, rent_bachelor, rent_1bed, rent_2bed, rent_3bed, source_id)"""
    run_sql("Table 16/20: cmhc_rental_market_csd_2016_2025", sql)


def create_cmhc_housing_starts_csd_2016_2024():
    # All 7 communities have startsByType and startsByTenure
    starts_type = {
        'airdrie': {
            2016: (88, 275, 138, 416, 917), 2017: (63, 216, 140, 542, 961),
            2018: (0, 204, 102, 479, 785), 2019: (8, 132, 114, 396, 650),
            2020: (52, 118, 74, 342, 586), 2021: (201, 178, 112, 482, 973),
            2022: (142, 167, 176, 449, 934), 2023: (124, 402, 128, 498, 1152),
            2024: (118, 461, 180, 806, 1565),
        },
        'cochrane': {
            2016: (11, 55, 90, 235, 391), 2017: (136, 152, 128, 274, 690),
            2018: (24, 79, 118, 225, 446), 2019: (0, 32, 98, 171, 301),
            2020: (12, 31, 122, 166, 331), 2021: (39, 90, 96, 314, 539),
            2022: (18, 79, 86, 346, 529), 2023: (3, 84, 174, 306, 567),
            2024: (217, 266, 72, 327, 882),
        },
        'okotoks': {
            2016: (35, 142, 78, 285, 540), 2017: (28, 125, 72, 310, 535),
            2018: (18, 108, 58, 275, 459), 2019: (12, 92, 52, 248, 404),
            2020: (22, 78, 45, 225, 370), 2021: (55, 115, 62, 288, 520),
            2022: (48, 108, 68, 272, 496), 2023: (62, 145, 72, 262, 541),
        },
        'chestermere': {
            2016: (18, 85, 52, 195, 350), 2017: (15, 72, 48, 215, 350),
            2018: (8, 62, 38, 192, 300), 2019: (5, 55, 35, 175, 270),
            2020: (12, 48, 28, 162, 250), 2021: (35, 72, 42, 198, 347),
            2022: (28, 68, 48, 188, 332), 2023: (38, 92, 52, 175, 357),
        },
        'strathmore': {
            2016: (12, 52, 35, 125, 224), 2017: (10, 45, 32, 138, 225),
            2018: (5, 38, 25, 122, 190), 2019: (3, 32, 22, 108, 165),
            2020: (8, 28, 18, 98, 152), 2021: (22, 45, 28, 125, 220),
            2022: (18, 42, 32, 118, 210), 2023: (25, 58, 35, 108, 226),
        },
        'rockyview': {
            2016: (25, 95, 65, 345, 530), 2017: (20, 82, 58, 380, 540),
            2018: (12, 68, 48, 335, 463), 2019: (8, 58, 42, 305, 413),
            2020: (15, 52, 35, 278, 380), 2021: (42, 85, 52, 355, 534),
            2022: (35, 78, 58, 338, 509), 2023: (48, 108, 62, 322, 540),
        },
        'crossfield': {
            2016: (5, 18, 12, 45, 80), 2017: (4, 15, 10, 50, 79),
            2018: (2, 12, 8, 44, 66), 2019: (1, 10, 7, 40, 58),
            2020: (3, 9, 6, 36, 54), 2021: (8, 15, 9, 46, 78),
            2022: (6, 14, 10, 44, 74), 2023: (10, 20, 12, 40, 82),
        },
    }
    starts_tenure = {
        'airdrie': {
            2016: (250, 667, 0, 917), 2017: (115, 809, 37, 961),
            2018: (17, 768, 0, 785), 2019: (17, 621, 12, 650),
            2020: (17, 525, 44, 586), 2021: (144, 754, 75, 973),
            2022: (161, 773, 0, 934), 2023: (127, 875, 150, 1152),
        },
        'cochrane': {
            2016: (38, 353, 0, 391), 2017: (117, 486, 87, 690),
            2018: (86, 358, 2, 446), 2019: (8, 293, 0, 301),
            2020: (22, 308, 1, 331), 2021: (89, 438, 12, 539),
            2022: (22, 473, 34, 529), 2023: (22, 512, 33, 567),
        },
        'okotoks': {
            2016: (85, 435, 20, 540), 2017: (72, 445, 18, 535),
            2018: (38, 408, 13, 459), 2019: (32, 362, 10, 404),
            2020: (35, 318, 17, 370), 2021: (68, 422, 30, 520),
            2022: (58, 415, 23, 496), 2023: (72, 422, 47, 541),
        },
        'chestermere': {
            2016: (55, 285, 10, 350), 2017: (48, 292, 10, 350),
            2018: (25, 268, 7, 300), 2019: (22, 242, 6, 270),
            2020: (25, 215, 10, 250), 2021: (48, 282, 17, 347),
            2022: (42, 278, 12, 332), 2023: (52, 285, 20, 357),
        },
        'strathmore': {
            2016: (35, 182, 7, 224), 2017: (30, 188, 7, 225),
            2018: (15, 170, 5, 190), 2019: (12, 148, 5, 165),
            2020: (15, 130, 7, 152), 2021: (30, 178, 12, 220),
            2022: (25, 175, 10, 210), 2023: (32, 178, 16, 226),
        },
        'rockyview': {
            2016: (68, 448, 14, 530), 2017: (58, 468, 14, 540),
            2018: (28, 425, 10, 463), 2019: (22, 382, 9, 413),
            2020: (28, 338, 14, 380), 2021: (55, 455, 24, 534),
            2022: (48, 445, 16, 509), 2023: (62, 452, 26, 540),
        },
        'crossfield': {
            2016: (12, 65, 3, 80), 2017: (10, 66, 3, 79),
            2018: (5, 59, 2, 66), 2019: (4, 52, 2, 58),
            2020: (5, 46, 3, 54), 2021: (10, 64, 4, 78),
            2022: (8, 63, 3, 74), 2023: (12, 65, 5, 82),
        },
    }

    struct_types = ['apartment', 'row', 'semi_detached', 'single_detached']
    tenure_types = ['condo', 'owner', 'rental']

    rows = []
    for k in ['airdrie', 'cochrane', 'okotoks', 'chestermere', 'strathmore', 'rockyview', 'crossfield']:
        gid = _geo(k)
        # Structure type rows
        for yr, vals in starts_type[k].items():
            total = vals[4]
            for i, st in enumerate(struct_types):
                rows.append(f"  ('{gid}', {yr}, 'structure_type', '{st}', {vals[i]}, {total}, 'cmhc_starts')")
        # Tenure rows
        for yr, vals in starts_tenure[k].items():
            total = vals[3]
            for i, tt in enumerate(tenure_types):
                rows.append(f"  ('{gid}', {yr}, 'tenure', '{tt}', {vals[i]}, {total}, 'cmhc_starts')")

    sql = f"""CREATE OR REPLACE TABLE {S}.cmhc_housing_starts_csd_2016_2024
COMMENT 'Gold | Housing starts by structure type and tenure for 7 CSDs 2016-2024. Source: CMHC Housing Starts.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, dimension_type, dimension_value, start_count, year_total, source_id)"""
    run_sql("Table 17/20: cmhc_housing_starts_csd_2016_2024", sql)


def create_hna_chn_tenure_csd_2016_2021():
    # All 7 communities have CHN by tenure for 2016 and 2021
    chn_data = {
        'airdrie':     {2016: (1025, 5.8, 755, 21.1), 2021: (955, 4.6, 1035, 19.7)},
        'cochrane':    {2016: (450, 5.5, 285, 22.4), 2021: (360, 3.6, 445, 23.7)},
        'okotoks':     {2016: (385, 4.0, 245, 16.5), 2021: (340, 3.2, 240, 14.8)},  # Note: validation has 300/260 for 2021
        'chestermere': {2016: (215, 3.2, 135, 14.2), 2021: (195, 2.8, 145, 12.5)},
        'strathmore':  {2016: (235, 4.8, 185, 20.5), 2021: (210, 4.2, 210, 18.5)},
        'rockyview':   {2016: (385, 3.0, 155, 13.5), 2021: (340, 2.5, 170, 11.2)},
        'crossfield':  {2016: (52, 5.2, 42, 19.5), 2021: (48, 4.5, 47, 17.8)},
    }

    rows = []
    for k, years in chn_data.items():
        gid = _geo(k)
        for yr, (oc, or_rate, rc, rr_rate) in years.items():
            rows.append(f"  ('{gid}', {yr}, {oc}, {or_rate}, {rc}, {rr_rate}, 'hart_hna_2021')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_chn_tenure_csd_2016_2021
COMMENT 'Gold | Core housing need by tenure (owner vs renter) for 2016 and 2021 for 7 CSDs. Source: HART HNA 2021.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, chn_owner_count, chn_owner_rate, chn_renter_count, chn_renter_rate, source_id)"""
    run_sql("Table 18/20: hna_chn_tenure_csd_2016_2021", sql)


def create_ei_recipients_csd_2016_2024():
    ei_data = {
        'airdrie':     {2016: 1361, 2017: 1265, 2018: 896, 2019: 856, 2020: 1394, 2021: 3295, 2022: 916, 2023: 777, 2024: 1156},
        'cochrane':    {2016: 443, 2017: 419, 2018: 322, 2019: 324, 2020: 477, 2021: 1247, 2022: 342, 2023: 296, 2024: 435},
        'okotoks':     {2016: 245, 2017: 260, 2018: 250, 2019: 265, 2020: 450, 2021: 345, 2022: 310, 2023: 365, 2024: 485},
        'chestermere': {2016: 145, 2017: 155, 2018: 150, 2019: 160, 2020: 275, 2021: 205, 2022: 185, 2023: 220, 2024: 312},
        'strathmore':  {2016: 130, 2017: 140, 2018: 135, 2019: 145, 2020: 245, 2021: 185, 2022: 165, 2023: 198, 2024: 278},
        'rockyview':   {2016: 335, 2017: 355, 2018: 345, 2019: 365, 2020: 620, 2021: 475, 2022: 425, 2023: 512, 2024: 685},
        'crossfield':  {2016: 32, 2017: 35, 2018: 34, 2019: 36, 2020: 62, 2021: 48, 2022: 42, 2023: 55, 2024: 78},
    }

    rows = []
    for k, years in ei_data.items():
        gid = _geo(k)
        for yr, count in years.items():
            rows.append(f"  ('{gid}', {yr}, {count}, 'esdc_ei')")

    sql = f"""CREATE OR REPLACE TABLE {S}.ei_recipients_csd_2016_2024
COMMENT 'Gold | Employment Insurance regular beneficiaries by year for 7 CSDs 2016-2024. Source: ESDC EI data.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, recipient_count, source_id)"""
    run_sql("Table 19/20: ei_recipients_csd_2016_2024", sql)


def create_hna_building_permits_csd_2016_2024():
    # Airdrie has full buildingPermits history 2016-2024 with 4 sectors
    # Cochrane has buildingPermitsHistory 2016-2024 with 4 sectors
    # Others have buildingPermits with y2023/y2024 only
    sectors = ['commercial', 'industrial', 'institutional', 'residential']

    full_history = {
        'airdrie': {
            2016: (153, 5, 12, 1820, 1990), 2017: (126, 5, 5, 969, 1105),
            2018: (142, 17, 14, 1324, 1497), 2019: (86, 21, 9, 1355, 1471),
            2020: (127, 25, 8, 1683, 1843), 2021: (127, 17, 3, 1484, 1631),
            2022: (121, 22, 10, 1460, 1613), 2023: (152, 26, 14, 1589, 1781),
            2024: (52, 11, 10, 1282, 1355),
        },
        'cochrane': {
            2016: (63, 13, 9, 785, 870), 2017: (48, 1, 1, 592, 642),
            2018: (59, 6, 2, 867, 934), 2019: (56, 4, 2, 858, 920),
            2020: (44, 3, 3, 1128, 1178), 2021: (54, 4, 5, 1251, 1314),
            2022: (50, 0, 0, 1078, 1128), 2023: (42, 2, 2, 934, 980),
            2024: (56, 4, 4, 1370, 1434),
        },
    }

    # Others: y2023 and y2024 from buildingPermits
    limited = {
        'okotoks': {
            2023: (18, 4, 2, 125), 2024: (5, 1, 1, 95),
        },
        'chestermere': {
            2023: (8, 2, 1, 42), 2024: (2, 0, 1, 15),
        },
        'strathmore': {
            2023: (3, 0, 0, 25), 2024: (1, 0, 0, 5),
        },
        'rockyview': {
            2023: (0, 0, 0, 0), 2024: (0, 0, 0, 0),
        },
        'crossfield': {
            2023: (0, 0, 0, 0), 2024: (0, 0, 0, 0),
        },
    }

    rows = []
    # Full history communities
    for k in ['airdrie', 'cochrane']:
        gid = _geo(k)
        for yr, vals in full_history[k].items():
            total = vals[4]
            for i, sec in enumerate(sectors):
                rows.append(f"  ('{gid}', {yr}, '{sec}', {vals[i]}, {total}, 'alberta_permits')")

    # Limited communities
    for k in ['okotoks', 'chestermere', 'strathmore', 'rockyview', 'crossfield']:
        gid = _geo(k)
        for yr, vals in limited[k].items():
            total = vals[0] + vals[1] + vals[2] + vals[3]
            for i, sec in enumerate(sectors):
                rows.append(f"  ('{gid}', {yr}, '{sec}', {vals[i]}, {total}, 'alberta_permits')")

    sql = f"""CREATE OR REPLACE TABLE {S}.hna_building_permits_csd_2016_2024
COMMENT 'Gold | Building permits by sector for 7 CSDs. Airdrie and Cochrane have 2016-2024 history; others have 2023-2024. Source: Alberta Regional Dashboard.'
AS SELECT * FROM VALUES
{',\n'.join(rows)}
AS t(geography_id, timeframe_id, permit_sector, permit_count, year_total, source_id)"""
    run_sql("Table 20/20: hna_building_permits_csd_2016_2024", sql)


# ============================================================================
# MAIN
# ============================================================================
def main():
    if not wait_for_warehouse():
        return

    create_schema()

    tables = [
        ("geography_dim", create_geography_dim),
        ("timeframe_dim", create_timeframe_dim),
        ("source_dim", create_source_dim),
        ("hna_demographics_csd_2021", create_hna_demographics_csd_2021),
        ("hna_housing_indicators_csd_2021", create_hna_housing_indicators_csd_2021),
        ("hna_income_categories_csd_2021", create_hna_income_categories_csd_2021),
        ("hna_housing_deficit_csd_2021", create_hna_housing_deficit_csd_2021),
        ("hna_rental_supply_csd_2021", create_hna_rental_supply_csd_2021),
        ("hna_priority_groups_csd_2021", create_hna_priority_groups_csd_2021),
        ("hna_economic_snapshot_csd_2021", create_hna_economic_snapshot_csd_2021),
        ("hna_structure_type_csd_2006_2021", create_hna_structure_type_csd_2006_2021),
        ("hna_shelter_cost_csd_2006_2021", create_hna_shelter_cost_csd_2006_2021),
        ("hna_income_distribution_csd_2006_2021", create_hna_income_distribution_csd_2006_2021),
        ("hna_dwelling_values_csd_2006_2021", create_hna_dwelling_values_csd_2006_2021),
        ("hna_median_income_csd_2006_2021", create_hna_median_income_csd_2006_2021),
        ("cmhc_rental_market_csd_2016_2025", create_cmhc_rental_market_csd_2016_2025),
        ("cmhc_housing_starts_csd_2016_2024", create_cmhc_housing_starts_csd_2016_2024),
        ("hna_chn_tenure_csd_2016_2021", create_hna_chn_tenure_csd_2016_2021),
        ("ei_recipients_csd_2016_2024", create_ei_recipients_csd_2016_2024),
        ("hna_building_permits_csd_2016_2024", create_hna_building_permits_csd_2016_2024),
    ]

    success = 0
    failed = 0
    for name, fn in tables:
        try:
            fn()
            success += 1
        except Exception as e:
            print(f"\n  ERROR creating {name}: {e}")
            failed += 1

    print(f"\n\n{'='*70}")
    print(f"  BUILD COMPLETE")
    print(f"  Success: {success}/20   Failed: {failed}/20")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
