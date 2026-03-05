"""Rebuild client_projects.mfah: drop old materialized tables, create views from public_data.housing."""
import subprocess, json, sys

TOKEN = sys.argv[1] if len(sys.argv) > 1 else None
if not TOKEN:
    print("Usage: python mfah_rebuild_gold.py <TOKEN>")
    sys.exit(1)

HOST = "https://adb-1169784117228619.19.azuredatabricks.net"
WH = "a7e9ada5cd37e1c7"

def q(sql):
    cmd = ['curl', '-s', '-X', 'POST', f'{HOST}/api/2.0/sql/statements',
        '-H', f'Authorization: Bearer {TOKEN}', '-H', 'Content-Type: application/json',
        '-d', json.dumps({'warehouse_id': WH, 'statement': sql, 'wait_timeout': '30s'})]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    d = json.loads(r.stdout)
    st = d.get('status', {}).get('state', '?')
    if st == 'SUCCEEDED':
        return 'OK'
    return 'FAIL: ' + d.get('status', {}).get('error', {}).get('message', '?')[:100]

# 1. Drop old materialized tables
old = [
    'hna_demographics_csd_2021', 'hna_housing_indicators_csd_2021', 'hna_income_categories_csd_2021',
    'hna_housing_deficit_csd_2021', 'hna_rental_supply_csd_2021', 'hna_priority_groups_csd_2021',
    'hna_economic_snapshot_csd_2021', 'hna_structure_type_csd_2006_2021', 'hna_shelter_cost_csd_2006_2021',
    'hna_income_distribution_csd_2006_2021', 'hna_dwelling_values_csd_2006_2021',
    'hna_median_income_csd_2006_2021', 'cmhc_rental_market_csd_2016_2025',
    'cmhc_housing_starts_csd_2016_2024', 'hna_chn_tenure_csd_2016_2021',
    'ei_recipients_csd_2016_2024', 'hna_building_permits_csd_2016_2024',
    'geography_dim', 'timeframe_dim', 'source_dim'
]

print("Dropping old materialized tables...")
for t in old:
    r = q(f'DROP TABLE IF EXISTS client_projects.mfah.{t}')
    print(f'  {t}: {r}')

# 2. Create views from public_data.housing
print("\nCreating views from public_data.housing...")

views = {
    'v_geography': """CREATE OR REPLACE VIEW client_projects.mfah.v_geography
        COMMENT 'View | MFAH geography dimension. Source: public_data.housing.hna_geography_dim'
        AS SELECT * FROM public_data.housing.hna_geography_dim""",

    'v_demographics': """CREATE OR REPLACE VIEW client_projects.mfah.v_demographics
        COMMENT 'View | Demographics (pop, age, income, housing). Source: Census 2006/2011 via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'demographics'""",

    'v_core_housing_need': """CREATE OR REPLACE VIEW client_projects.mfah.v_core_housing_need
        COMMENT 'View | Core housing need by tenure and income. Source: HART HNA 2021 via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'core_housing_need'""",

    'v_affordability': """CREATE OR REPLACE VIEW client_projects.mfah.v_affordability
        COMMENT 'View | Affordability thresholds (AMHI, shelter costs). Source: HART HNA 2021 via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'affordability'""",

    'v_housing_supply': """CREATE OR REPLACE VIEW client_projects.mfah.v_housing_supply
        COMMENT 'View | Rental composition, subsidized, vacancy. Source: HART HNA 2021 via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'housing_supply'""",

    'v_growth_projections': """CREATE OR REPLACE VIEW client_projects.mfah.v_growth_projections
        COMMENT 'View | 2031 household projections by size and income. Source: HART HNA 2021 via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'growth_projections'""",

    'v_economic_indicators': """CREATE OR REPLACE VIEW client_projects.mfah.v_economic_indicators
        COMMENT 'View | Building permits, EI recipients. Source: Alberta Regional Dashboard via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'economic_indicators'""",

    'v_priority_populations': """CREATE OR REPLACE VIEW client_projects.mfah.v_priority_populations
        COMMENT 'View | CHN rates for 18 priority groups. Source: HART HNA 2021 via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'priority_populations'""",

    'v_rental_market': """CREATE OR REPLACE VIEW client_projects.mfah.v_rental_market
        COMMENT 'View | Rent, vacancy, starts, completions. Source: CMHC + HART + Alberta via hna_dashboard'
        AS SELECT * FROM public_data.housing.hna_dashboard_2006_2031
        WHERE dashboard_section = 'rental_market'""",
}

for name, sql in views.items():
    r = q(sql)
    print(f'  {name}: {r}')

# 3. Verify
print("\nFinal state of client_projects.mfah:")
cmd = ['curl', '-s', '-X', 'POST', f'{HOST}/api/2.0/sql/statements',
    '-H', f'Authorization: Bearer {TOKEN}', '-H', 'Content-Type: application/json',
    '-d', json.dumps({'warehouse_id': WH, 'statement': 'SHOW TABLES IN client_projects.mfah', 'wait_timeout': '30s'})]
r = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
d = json.loads(r.stdout)
if d.get('status', {}).get('state') == 'SUCCEEDED':
    rows = d['result']['data_array']
    print(f'  {len(rows)} objects:')
    for row in rows:
        print(f'    {row[1]}')
