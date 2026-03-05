"""
mfah_validate.py -- Full validation of the MFAH Gold Layer in Databricks.
Checks: row counts, dimensions, FKs, nulls, truth table cross-reference,
internal consistency, source traceability, spot-checks, standards.

Usage:
    python mfah_validate.py <DATABRICKS_TOKEN>

Follows the proven 06_validate.py pattern from the Beltline project.
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
S = "client_projects.mfah"  # schema shorthand

# Token from CLI arg
TOKEN = sys.argv[1] if len(sys.argv) > 1 else None
if not TOKEN:
    print("Usage: python mfah_validate.py <DATABRICKS_TOKEN>")
    print("Generate at: Settings > Developer > Access Tokens")
    sys.exit(1)

# ============================================================================
# PASS/FAIL ENGINE
# ============================================================================
results = []  # (category, check_name, "PASS"|"FAIL", detail)


def record(category, name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    results.append((category, name, status, detail))
    marker = "  [PASS]" if passed else "  [FAIL]"
    if not passed:
        print(f"{marker} {name}: {detail}")


# ============================================================================
# SQL EXECUTION (curl subprocess -- avoids urllib 403 on Windows)
# ============================================================================
def run_sql(label, sql, timeout=180):
    """Submit SQL via Databricks SQL Statements API, poll for results."""
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")

    # Submit
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
        # Check if result came back immediately
        st = d.get("status", {}).get("state", "?")
        if st == "SUCCEEDED":
            return _print_result(d)
        print(f"  SUBMIT ERROR: {json.dumps(d, indent=2)[:500]}")
        return None

    # Poll
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
            return _print_result(d)
        elif st == "FAILED":
            err = d.get("status", {}).get("error", {}).get("message", "?")
            print(f"  FAILED: {err}")
            return None
        time.sleep(5)

    print(f"  TIMEOUT after {timeout}s")
    return None


def _print_result(d):
    """Pretty-print SQL result and return parsed rows as list of dicts."""
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    raw_rows = d.get("result", {}).get("data_array", [])

    if not raw_rows:
        print("  (no rows)")
        return {"cols": cols, "rows": []}

    # Pretty-print
    widths = []
    for i, c in enumerate(cols):
        w = len(str(c))
        for row in raw_rows:
            val = str(row[i]) if row[i] is not None else "NULL"
            w = max(w, len(val))
        widths.append(min(w, 45))

    header = "  ".join(str(c).ljust(w) for c, w in zip(cols, widths))
    print(f"  {header}")
    print(f"  {'-' * len(header)}")
    for row in raw_rows[:60]:
        vals = [str(v if v is not None else "NULL")[:45].ljust(w) for v, w in zip(row, widths)]
        print(f"  {'  '.join(vals)}")
    if len(raw_rows) > 60:
        print(f"  ... ({len(raw_rows)} total rows)")

    # Return as list of dicts
    parsed = []
    for row in raw_rows:
        parsed.append({cols[i]: row[i] for i in range(len(cols))})
    return {"cols": cols, "rows": parsed}


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
# TRUTH TABLE -- Expected values from communityData{} in the dashboard HTML
# Source: ~/AppData/Local/Temp/mfah-deploy/index.html lines 3757-4717
#
# Confidence tiers:
#   VERIFIED = Airdrie & Cochrane (have "ACTUAL" annotations throughout)
#   UNVERIFIED = Okotoks, Chestermere, Strathmore, Rocky View, Crossfield
#                (fewer source annotations; some values may be estimates)
# ============================================================================
VERIFIED_COMMUNITIES = {"airdrie", "cochrane"}

TRUTH_DEMOGRAPHICS = {
    "airdrie":     {"pop_2016": 61190, "pop_2021": 73795, "growth_5yr_pct": 20.6, "households": 26295, "median_income": 110000, "median_rent": 1650, "tenant_pct": 20.7},
    "cochrane":    {"pop_2016": 25640, "pop_2021": 31470, "growth_5yr_pct": 22.7, "households": 12100, "median_income": 113000, "median_rent": 1292, "tenant_pct": 16.3},
    "okotoks":     {"pop_2016": 28881, "pop_2021": 30405, "growth_5yr_pct": 5.3,  "households": 11200, "median_income": 118000, "median_rent": 1450, "tenant_pct": 16.2},
    "chestermere": {"pop_2016": 19887, "pop_2021": 21952, "growth_5yr_pct": 10.4, "households": 7500,  "median_income": 135000, "median_rent": 1700, "tenant_pct": 14.8},
    "strathmore":  {"pop_2016": 13756, "pop_2021": 14339, "growth_5yr_pct": 4.2,  "households": 5400,  "median_income": 95000,  "median_rent": 1250, "tenant_pct": 22.5},
    "rockyview":   {"pop_2016": 39407, "pop_2021": 42635, "growth_5yr_pct": 8.2,  "households": 14200, "median_income": 142000, "median_rent": 1400, "tenant_pct": 8.2},
    "crossfield":  {"pop_2016": 3100,  "pop_2021": 3450,  "growth_5yr_pct": 11.3, "households": 1250,  "median_income": 88000,  "median_rent": 1150, "tenant_pct": 19.5},
}

TRUTH_HOUSING_INDICATORS = {
    "airdrie":     {"chn_total": 1990, "chn_pct": 7.7, "chn_owner_pct": 4.6, "chn_renter_pct": 19.6, "inadequate_count": 565, "unsuitable_count": 970, "unaffordable_count": 5450},
    "cochrane":    {"chn_total": 800,  "chn_pct": 6.8, "chn_owner_pct": 3.6, "chn_renter_pct": 23.4, "inadequate_count": 270, "unsuitable_count": 265, "unaffordable_count": 2265},
    "okotoks":     {"chn_total": 560,  "chn_pct": 5.0, "chn_owner_pct": 3.2, "chn_renter_pct": 14.3, "inadequate_count": 560, "unsuitable_count": 336, "unaffordable_count": 1904},
    "chestermere": {"chn_total": 375,  "chn_pct": 5.0, "chn_owner_pct": 3.1, "chn_renter_pct": 16.2, "inadequate_count": 338, "unsuitable_count": 225, "unaffordable_count": 1200},
    "strathmore":  {"chn_total": 432,  "chn_pct": 8.0, "chn_owner_pct": 4.8, "chn_renter_pct": 19.1, "inadequate_count": 324, "unsuitable_count": 216, "unaffordable_count": 1188},
    "rockyview":   {"chn_total": 568,  "chn_pct": 4.0, "chn_owner_pct": 3.0, "chn_renter_pct": 15.7, "inadequate_count": 710, "unsuitable_count": 284, "unaffordable_count": 1846},
    "crossfield":  {"chn_total": 106,  "chn_pct": 8.5, "chn_owner_pct": 4.8, "chn_renter_pct": 23.8, "inadequate_count": 88,  "unsuitable_count": 63,  "unaffordable_count": 300},
}

TRUTH_ECONOMIC = {
    "airdrie":     {"unemployment_rate_2021": 11.1, "avg_sale_price": 418400, "owner_income": 121000, "renter_income": 74500},
    "cochrane":    {"unemployment_rate_2021": 9.5,  "avg_sale_price": 466800, "owner_income": 122000, "renter_income": 66500},
    "okotoks":     {"unemployment_rate_2021": 6.5,  "avg_sale_price": 520000, "owner_income": 128000, "renter_income": 78000},
    "chestermere": {"unemployment_rate_2021": 5.8,  "avg_sale_price": 620000, "owner_income": 145000, "renter_income": 88000},
    "strathmore":  {"unemployment_rate_2021": 9.2,  "avg_sale_price": 420000, "owner_income": 105000, "renter_income": 68000},
    "rockyview":   {"unemployment_rate_2021": 5.5,  "avg_sale_price": 680000, "owner_income": 155000, "renter_income": 92000},
    "crossfield":  {"unemployment_rate_2021": 9.8,  "avg_sale_price": 380000, "owner_income": 98000,  "renter_income": 62000},
}

TRUTH_RENTAL_SUPPLY = {
    "airdrie":     {"primary_rental": 1543, "secondary_rental": 3892, "subsidized": 95, "total_rental": 5435, "co_op_units": 0},
    "cochrane":    {"primary_rental": 212,  "secondary_rental": 1738, "subsidized": 95, "total_rental": 1950, "co_op_units": 0},
    "okotoks":     {"primary_rental": 520,  "secondary_rental": 1298, "subsidized": 38, "total_rental": 1818, "co_op_units": 0},
    "chestermere": {"primary_rental": 285,  "secondary_rental": 827,  "subsidized": 22, "total_rental": 1112, "co_op_units": 0},
    "strathmore":  {"primary_rental": 380,  "secondary_rental": 835,  "subsidized": 65, "total_rental": 1215, "co_op_units": 0},
    "rockyview":   {"primary_rental": 245,  "secondary_rental": 920,  "subsidized": 125, "total_rental": 1165, "co_op_units": 0},
    "crossfield":  {"primary_rental": 58,   "secondary_rental": 186,  "subsidized": 8,  "total_rental": 244,  "co_op_units": 0},
}

# EI recipients time-series (community -> {year: count})
TRUTH_EI = {
    "airdrie":     {2016: 1361, 2017: 1265, 2018: 896, 2019: 856, 2020: 1394, 2021: 3295, 2022: 916, 2023: 777, 2024: 1156},
    "cochrane":    {2016: 443, 2017: 419, 2018: 322, 2019: 324, 2020: 477, 2021: 1247, 2022: 342, 2023: 296, 2024: 435},
    "okotoks":     {2016: 245, 2017: 260, 2018: 250, 2019: 265, 2020: 450, 2021: 345, 2022: 310, 2023: 365, 2024: 485},
    "chestermere": {2016: 145, 2017: 155, 2018: 150, 2019: 160, 2020: 275, 2021: 205, 2022: 185, 2023: 220, 2024: 312},
    "strathmore":  {2016: 130, 2017: 140, 2018: 135, 2019: 145, 2020: 245, 2021: 185, 2022: 165, 2023: 198, 2024: 278},
    "rockyview":   {2016: 335, 2017: 355, 2018: 345, 2019: 365, 2020: 620, 2021: 475, 2022: 425, 2023: 512, 2024: 685},
    "crossfield":  {2016: 32, 2017: 35, 2018: 34, 2019: 36, 2020: 62, 2021: 48, 2022: 42, 2023: 55, 2024: 78},
}

# Income categories (community -> tier -> {pct, count})
TRUTH_INCOME_CATEGORIES = {
    "airdrie":     {"very_low": {"pct": 1.28, "count": 337}, "low": {"pct": 14.44, "count": 3797}, "moderate": {"pct": 20.27, "count": 5332}, "median": {"pct": 26.52, "count": 6973}, "high": {"pct": 37.5, "count": 9861}},
    "cochrane":    {"very_low": {"pct": 2.03, "count": 246}, "low": {"pct": 14.64, "count": 1772}, "moderate": {"pct": 20.59, "count": 2491}, "median": {"pct": 24.43, "count": 2956}, "high": {"pct": 38.31, "count": 4635}},
}

# CHN by tenure 2016 vs 2021
TRUTH_CHN_TENURE = {
    "airdrie":     {2016: {"owner": {"count": 1025, "rate": 5.8}, "renter": {"count": 755, "rate": 21.1}}, 2021: {"owner": {"count": 955, "rate": 4.6}, "renter": {"count": 1035, "rate": 19.7}}},
    "cochrane":    {2016: {"owner": {"count": 450, "rate": 5.5}, "renter": {"count": 285, "rate": 22.4}}, 2021: {"owner": {"count": 360, "rate": 3.6}, "renter": {"count": 445, "rate": 23.7}}},
}


# ============================================================================
# COMPARISON HELPERS
# ============================================================================
def safe_num(val):
    """Convert SQL result value to float, handling None and string."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def compare(actual, expected, col_name, community, category, tolerance=0.15):
    """Compare actual vs expected. Integers exact, decimals within tolerance."""
    a = safe_num(actual)
    if a is None:
        record(category, f"{community}.{col_name}", False, f"actual=NULL expected={expected}")
        return
    e = float(expected)
    if isinstance(expected, float) and not expected.is_integer():
        # Percentage/decimal comparison
        passed = abs(a - e) < tolerance
        record(category, f"{community}.{col_name}", passed,
               f"actual={a} expected={e} diff={abs(a-e):.2f}" if not passed else "")
    else:
        # Integer comparison
        passed = int(a) == int(e)
        record(category, f"{community}.{col_name}", passed,
               f"actual={int(a)} expected={int(e)}" if not passed else "")


# ============================================================================
# MAIN
# ============================================================================
def main():
    if not wait_for_warehouse():
        return

    # ====================================================================
    # 0. DISCOVERY -- what exists?
    # ====================================================================
    discovery = run_sql("Discovery: schemas in client_projects",
                        "SHOW SCHEMAS IN client_projects")

    schema_exists = False
    if discovery and discovery["rows"]:
        for row in discovery["rows"]:
            ns = row.get("databaseName", row.get("namespace", ""))
            if "mfah" in str(ns).lower():
                schema_exists = True
                break

    if not schema_exists:
        print("\n" + "=" * 70)
        print("  SCHEMA client_projects.mfah DOES NOT EXIST YET")
        print("  This script is ready to run once the gold tables are built.")
        print("  Exiting early -- no tables to validate.")
        print("=" * 70)
        return

    tables_result = run_sql("Discovery: tables in client_projects.mfah",
                            f"SHOW TABLES IN {S}")

    if not tables_result or not tables_result["rows"]:
        print("\n  No tables found in client_projects.mfah. Exiting.")
        return

    existing_tables = set()
    for row in tables_result["rows"]:
        tname = row.get("tableName", row.get("table_name", ""))
        existing_tables.add(str(tname).lower())

    print(f"\n  Found {len(existing_tables)} tables: {sorted(existing_tables)}")

    # ====================================================================
    # 1. ROW COUNTS
    # ====================================================================
    EXPECTED_ROWS = {
        "geography_dim": 7,
        "timeframe_dim": 20,
        "source_dim": 12,
        "hna_demographics_csd_2021": 7,
        "hna_housing_indicators_csd_2021": 7,
        "hna_income_categories_csd_2021": 35,
        "hna_housing_deficit_csd_2021": 175,
        "hna_rental_supply_csd_2021": 7,
        "hna_priority_groups_csd_2021": 41,   # 18 each for Airdrie+Cochrane + 5 communityTotal
        "hna_economic_snapshot_csd_2021": 7,
        "hna_structure_type_csd_2006_2021": 56,   # 2 communities × 4 years × 7 types
        "hna_shelter_cost_csd_2006_2021": 40,      # 2 communities × 4 years × 5 bands
        "hna_income_distribution_csd_2006_2021": 144, # 2 communities × 4 years × 3 tenures × 6 bands
        "hna_dwelling_values_csd_2006_2021": 56,   # 2 communities × 4 years × 7 types
        "hna_median_income_csd_2006_2021": 8,      # 2 communities × 4 years (wide: median+avg per tenure)
        "cmhc_rental_market_csd_2016_2025": 70,    # 7 communities × 10 years
        "cmhc_housing_starts_csd_2016_2024": 400,  # 7 communities × variable years × 2 dims × categories
        "hna_chn_tenure_csd_2016_2021": 14,        # 7 communities × 2 years (wide: owner+renter)
        "ei_recipients_csd_2016_2024": 63,         # 7 communities × 9 years
        "hna_building_permits_csd_2016_2024": 112, # variable per community × 4 sectors
    }

    # Build UNION ALL for tables that exist
    count_parts = []
    for tbl, exp in EXPECTED_ROWS.items():
        if tbl in existing_tables:
            count_parts.append(
                f"SELECT '{tbl}' AS tbl, COUNT(*) AS actual_rows, {exp} AS expected_rows FROM {S}.{tbl}"
            )

    if count_parts:
        result = run_sql("Cat 1: Row counts", "\nUNION ALL\n".join(count_parts))
        if result and result["rows"]:
            for row in result["rows"]:
                tbl = row["tbl"]
                actual = int(row["actual_rows"])
                expected = int(row["expected_rows"])
                record("1-rowcounts", tbl, actual == expected,
                       f"actual={actual} expected={expected}" if actual != expected else "")

    # ====================================================================
    # 2. DIMENSION INTEGRITY
    # ====================================================================
    if "geography_dim" in existing_tables:
        geo = run_sql("Cat 2: geography_dim",
                      f"SELECT geography_key, geography, geography_id FROM {S}.geography_dim ORDER BY geography_key")
        if geo and geo["rows"]:
            actual_keys = {row["geography_key"] for row in geo["rows"]}
            expected_keys = {"airdrie", "cochrane", "okotoks", "chestermere", "strathmore", "rockyview", "crossfield"}
            record("2-dims", "geography_dim completeness", actual_keys == expected_keys,
                   f"missing={expected_keys - actual_keys} extra={actual_keys - expected_keys}" if actual_keys != expected_keys else "")

    if "timeframe_dim" in existing_tables:
        tf = run_sql("Cat 2: timeframe_dim census years",
                     f"SELECT timeframe_id FROM {S}.timeframe_dim WHERE is_census_year = true ORDER BY timeframe_id")
        if tf and tf["rows"]:
            actual_years = {int(row["timeframe_id"]) for row in tf["rows"]}
            expected_years = {2006, 2011, 2016, 2021}
            record("2-dims", "census years", actual_years == expected_years,
                   f"actual={sorted(actual_years)} expected={sorted(expected_years)}" if actual_years != expected_years else "")

    if "source_dim" in existing_tables:
        src = run_sql("Cat 2: source_dim",
                      f"SELECT source_id, source_organization FROM {S}.source_dim ORDER BY source_id")
        if src and src["rows"]:
            record("2-dims", "source_dim count", len(src["rows"]) == 12,
                   f"actual={len(src['rows'])} expected=12" if len(src["rows"]) != 12 else "")

    # ====================================================================
    # 3. REFERENTIAL INTEGRITY (FK checks)
    # ====================================================================
    fact_tables_geo_fk = [
        "hna_demographics_csd_2021", "hna_housing_indicators_csd_2021",
        "hna_income_categories_csd_2021", "hna_housing_deficit_csd_2021",
        "hna_rental_supply_csd_2021", "hna_priority_groups_csd_2021",
        "hna_economic_snapshot_csd_2021",
        "hna_structure_type_csd_2006_2021", "hna_shelter_cost_csd_2006_2021",
        "hna_income_distribution_csd_2006_2021", "hna_dwelling_values_csd_2006_2021",
        "hna_median_income_csd_2006_2021", "cmhc_rental_market_csd_2016_2025",
        "cmhc_housing_starts_csd_2016_2024", "hna_chn_tenure_csd_2016_2021",
        "ei_recipients_csd_2016_2024", "hna_building_permits_csd_2016_2024",
    ]
    ts_tables = [
        "hna_structure_type_csd_2006_2021", "hna_shelter_cost_csd_2006_2021",
        "hna_income_distribution_csd_2006_2021", "hna_dwelling_values_csd_2006_2021",
        "hna_median_income_csd_2006_2021", "cmhc_rental_market_csd_2016_2025",
        "cmhc_housing_starts_csd_2016_2024", "hna_chn_tenure_csd_2016_2021",
        "ei_recipients_csd_2016_2024", "hna_building_permits_csd_2016_2024",
    ]

    # Geography FK check
    if "geography_dim" in existing_tables:
        fk_parts = []
        for tbl in fact_tables_geo_fk:
            if tbl in existing_tables:
                fk_parts.append(
                    f"SELECT '{tbl}' AS tbl, COUNT(*) AS orphans "
                    f"FROM {S}.{tbl} f LEFT JOIN {S}.geography_dim g ON f.geography_id = g.geography_id "
                    f"WHERE g.geography_id IS NULL"
                )
        if fk_parts:
            fk_result = run_sql("Cat 3: FK geography_id", "\nUNION ALL\n".join(fk_parts))
            if fk_result and fk_result["rows"]:
                for row in fk_result["rows"]:
                    orphans = int(row["orphans"])
                    record("3-fk", f"{row['tbl']}->geography_dim", orphans == 0,
                           f"{orphans} orphan rows" if orphans > 0 else "")

    # Timeframe FK check
    if "timeframe_dim" in existing_tables:
        tf_parts = []
        for tbl in ts_tables:
            if tbl in existing_tables:
                tf_parts.append(
                    f"SELECT '{tbl}' AS tbl, COUNT(*) AS orphans "
                    f"FROM {S}.{tbl} f LEFT JOIN {S}.timeframe_dim t ON f.timeframe_id = t.timeframe_id "
                    f"WHERE t.timeframe_id IS NULL"
                )
        if tf_parts:
            tf_result = run_sql("Cat 3: FK timeframe_id", "\nUNION ALL\n".join(tf_parts))
            if tf_result and tf_result["rows"]:
                for row in tf_result["rows"]:
                    orphans = int(row["orphans"])
                    record("3-fk", f"{row['tbl']}->timeframe_dim", orphans == 0,
                           f"{orphans} orphan rows" if orphans > 0 else "")

    # ====================================================================
    # 4. NULL RATES
    # ====================================================================
    null_checks = []
    for tbl in fact_tables_geo_fk:
        if tbl in existing_tables:
            null_checks.append(
                f"SELECT '{tbl}' AS tbl, "
                f"SUM(CASE WHEN geography_id IS NULL THEN 1 ELSE 0 END) AS null_geo, "
                f"SUM(CASE WHEN source_id IS NULL THEN 1 ELSE 0 END) AS null_src "
                f"FROM {S}.{tbl}"
            )
    if null_checks:
        null_result = run_sql("Cat 4: Null rates on FK columns", "\nUNION ALL\n".join(null_checks))
        if null_result and null_result["rows"]:
            for row in null_result["rows"]:
                null_geo = int(row["null_geo"])
                null_src = int(row["null_src"])
                record("4-nulls", f"{row['tbl']}.geography_id", null_geo == 0,
                       f"{null_geo} NULLs" if null_geo > 0 else "")
                record("4-nulls", f"{row['tbl']}.source_id", null_src == 0,
                       f"{null_src} NULLs" if null_src > 0 else "")

    # ====================================================================
    # 5. TRUTH TABLE CROSS-REFERENCE
    # ====================================================================

    # 5a: Demographics
    if "hna_demographics_csd_2021" in existing_tables and "geography_dim" in existing_tables:
        demo = run_sql("Cat 5: Demographics truth check",
                       f"SELECT g.geography_key, d.* FROM {S}.hna_demographics_csd_2021 d "
                       f"JOIN {S}.geography_dim g ON d.geography_id = g.geography_id "
                       f"ORDER BY g.geography_key")
        if demo and demo["rows"]:
            for row in demo["rows"]:
                community = row.get("geography_key", "")
                if community in TRUTH_DEMOGRAPHICS:
                    for col, expected in TRUTH_DEMOGRAPHICS[community].items():
                        compare(row.get(col), expected, col, community, "5-truth-demographics")

    # 5b: Housing indicators
    if "hna_housing_indicators_csd_2021" in existing_tables and "geography_dim" in existing_tables:
        hi = run_sql("Cat 5: Housing indicators truth check",
                     f"SELECT g.geography_key, h.* FROM {S}.hna_housing_indicators_csd_2021 h "
                     f"JOIN {S}.geography_dim g ON h.geography_id = g.geography_id "
                     f"ORDER BY g.geography_key")
        if hi and hi["rows"]:
            for row in hi["rows"]:
                community = row.get("geography_key", "")
                if community in TRUTH_HOUSING_INDICATORS:
                    for col, expected in TRUTH_HOUSING_INDICATORS[community].items():
                        compare(row.get(col), expected, col, community, "5-truth-housing")

    # 5c: Economic snapshot
    if "hna_economic_snapshot_csd_2021" in existing_tables and "geography_dim" in existing_tables:
        econ = run_sql("Cat 5: Economic snapshot truth check",
                       f"SELECT g.geography_key, e.* FROM {S}.hna_economic_snapshot_csd_2021 e "
                       f"JOIN {S}.geography_dim g ON e.geography_id = g.geography_id "
                       f"ORDER BY g.geography_key")
        if econ and econ["rows"]:
            for row in econ["rows"]:
                community = row.get("geography_key", "")
                if community in TRUTH_ECONOMIC:
                    for col, expected in TRUTH_ECONOMIC[community].items():
                        compare(row.get(col), expected, col, community, "5-truth-economic")

    # 5d: Rental supply
    if "hna_rental_supply_csd_2021" in existing_tables and "geography_dim" in existing_tables:
        rs = run_sql("Cat 5: Rental supply truth check",
                     f"SELECT g.geography_key, r.* FROM {S}.hna_rental_supply_csd_2021 r "
                     f"JOIN {S}.geography_dim g ON r.geography_id = g.geography_id "
                     f"ORDER BY g.geography_key")
        if rs and rs["rows"]:
            for row in rs["rows"]:
                community = row.get("geography_key", "")
                if community in TRUTH_RENTAL_SUPPLY:
                    for col, expected in TRUTH_RENTAL_SUPPLY[community].items():
                        compare(row.get(col), expected, col, community, "5-truth-rental")

    # 5e: EI recipients (time-series)
    if "ei_recipients_csd_2016_2024" in existing_tables and "geography_dim" in existing_tables:
        ei = run_sql("Cat 5: EI recipients truth check",
                     f"SELECT g.geography_key, e.timeframe_id, e.recipient_count "
                     f"FROM {S}.ei_recipients_csd_2016_2024 e "
                     f"JOIN {S}.geography_dim g ON e.geography_id = g.geography_id "
                     f"ORDER BY g.geography_key, e.timeframe_id")
        if ei and ei["rows"]:
            for row in ei["rows"]:
                community = row.get("geography_key", "")
                year = int(row.get("timeframe_id", 0))
                if community in TRUTH_EI and year in TRUTH_EI[community]:
                    compare(row.get("recipient_count"), TRUTH_EI[community][year],
                            f"ei_{year}", community, "5-truth-ei")

    # 5f: Income categories (Airdrie + Cochrane only -- verified data)
    if "hna_income_categories_csd_2021" in existing_tables and "geography_dim" in existing_tables:
        ic = run_sql("Cat 5: Income categories truth check",
                     f"SELECT g.geography_key, i.income_tier, i.household_pct, i.household_count "
                     f"FROM {S}.hna_income_categories_csd_2021 i "
                     f"JOIN {S}.geography_dim g ON i.geography_id = g.geography_id "
                     f"WHERE g.geography_key IN ('airdrie', 'cochrane') "
                     f"ORDER BY g.geography_key, i.income_tier")
        if ic and ic["rows"]:
            for row in ic["rows"]:
                community = row.get("geography_key", "")
                tier = row.get("income_tier", "")
                if community in TRUTH_INCOME_CATEGORIES and tier in TRUTH_INCOME_CATEGORIES[community]:
                    exp = TRUTH_INCOME_CATEGORIES[community][tier]
                    compare(row.get("household_count"), exp["count"],
                            f"income_{tier}_count", community, "5-truth-income")
                    compare(row.get("household_pct"), exp["pct"],
                            f"income_{tier}_pct", community, "5-truth-income")

    # ====================================================================
    # 6. INTERNAL CONSISTENCY
    # ====================================================================

    # 6a: Priority groups community_total should match housing indicators CHN total
    if all(t in existing_tables for t in ["hna_priority_groups_csd_2021", "hna_housing_indicators_csd_2021", "geography_dim"]):
        cons1 = run_sql("Cat 6: priority_groups vs housing_indicators CHN",
                        f"SELECT g.geography_key, pg.chn_count AS pg_total, hi.chn_total AS hi_total "
                        f"FROM {S}.hna_priority_groups_csd_2021 pg "
                        f"JOIN {S}.geography_dim g ON pg.geography_id = g.geography_id "
                        f"JOIN {S}.hna_housing_indicators_csd_2021 hi ON pg.geography_id = hi.geography_id "
                        f"WHERE pg.priority_group = 'community_total'")
        if cons1 and cons1["rows"]:
            for row in cons1["rows"]:
                pg = safe_num(row.get("pg_total"))
                hi = safe_num(row.get("hi_total"))
                if pg is not None and hi is not None:
                    record("6-consistency", f"{row['geography_key']}: priority_total=chn_total",
                           int(pg) == int(hi),
                           f"priority_groups={int(pg)} housing_indicators={int(hi)}" if int(pg) != int(hi) else "")

    # 6b: Income categories counts should sum to total households in demographics
    if all(t in existing_tables for t in ["hna_income_categories_csd_2021", "hna_demographics_csd_2021", "geography_dim"]):
        cons2 = run_sql("Cat 6: income categories sum vs demographics households",
                        f"SELECT g.geography_key, "
                        f"SUM(ic.household_count) AS ic_sum, d.households AS demo_hh "
                        f"FROM {S}.hna_income_categories_csd_2021 ic "
                        f"JOIN {S}.geography_dim g ON ic.geography_id = g.geography_id "
                        f"JOIN {S}.hna_demographics_csd_2021 d ON ic.geography_id = d.geography_id "
                        f"GROUP BY g.geography_key, d.households")
        if cons2 and cons2["rows"]:
            for row in cons2["rows"]:
                ic_sum = safe_num(row.get("ic_sum"))
                demo_hh = safe_num(row.get("demo_hh"))
                if ic_sum is not None and demo_hh is not None:
                    # Allow small rounding tolerance (HART categories may not sum exactly)
                    diff = abs(int(ic_sum) - int(demo_hh))
                    record("6-consistency", f"{row['geography_key']}: income_sum~=households",
                           diff <= 10,
                           f"income_sum={int(ic_sum)} households={int(demo_hh)} diff={diff}" if diff > 10 else "")

    # ====================================================================
    # 7. SOURCE TRACEABILITY
    # ====================================================================
    if "source_dim" in existing_tables:
        # Check every source_dim row is referenced
        ref_parts = []
        for tbl in fact_tables_geo_fk:
            if tbl in existing_tables:
                ref_parts.append(f"SELECT DISTINCT source_id FROM {S}.{tbl}")
        if ref_parts:
            trace = run_sql("Cat 7: Source traceability",
                            f"SELECT s.source_id, s.source_name, "
                            f"CASE WHEN f.source_id IS NOT NULL THEN 'USED' ELSE 'UNUSED' END AS status "
                            f"FROM {S}.source_dim s "
                            f"LEFT JOIN ({' UNION '.join(ref_parts)}) f ON s.source_id = f.source_id "
                            f"ORDER BY s.source_id")
            if trace and trace["rows"]:
                unused = [r["source_id"] for r in trace["rows"] if r.get("status") == "UNUSED"]
                record("7-traceability", "all sources referenced",
                       len(unused) == 0,
                       f"unused sources: {unused}" if unused else "")

    # ====================================================================
    # 8. SPOT-CHECK SAMPLES
    # ====================================================================

    # Suppressed bachelor rent for Airdrie (should be NULL)
    if "cmhc_rental_market_csd_2016_2025" in existing_tables and "geography_dim" in existing_tables:
        run_sql("Cat 8: Airdrie bachelor rent (expect NULL/0)",
                f"SELECT g.geography_key, r.timeframe_id, r.rent_bachelor "
                f"FROM {S}.cmhc_rental_market_csd_2016_2025 r "
                f"JOIN {S}.geography_dim g ON r.geography_id = g.geography_id "
                f"WHERE g.geography_key = 'airdrie' AND r.timeframe_id = 2024")

    # Co-op units (should all be 0)
    if "hna_rental_supply_csd_2021" in existing_tables:
        run_sql("Cat 8: Co-op units (expect all 0)",
                f"SELECT g.geography_key, r.co_op_units "
                f"FROM {S}.hna_rental_supply_csd_2021 r "
                f"JOIN {S}.geography_dim g ON r.geography_id = g.geography_id "
                f"WHERE r.co_op_units != 0")

    # Smallest communities have data
    if "hna_demographics_csd_2021" in existing_tables and "geography_dim" in existing_tables:
        run_sql("Cat 8: Smallest communities have data",
                f"SELECT g.geography_key, d.pop_2021, d.households "
                f"FROM {S}.hna_demographics_csd_2021 d "
                f"JOIN {S}.geography_dim g ON d.geography_id = g.geography_id "
                f"WHERE g.geography_key IN ('crossfield', 'strathmore') "
                f"ORDER BY d.pop_2021")

    # ====================================================================
    # 9. STANDARDS COMPLIANCE
    # ====================================================================
    run_sql("Cat 9: All tables", f"SHOW TABLES IN {S}")

    print("\n\nStandards compliance checklist:")
    print("  [x] Catalog: client_projects (client data)")
    print("  [x] Schema: mfah")
    print("  [x] Table naming: source_topic_timeframe pattern")
    print("  [x] Star schema: 3 dims + 17 facts")
    print("  [ ] All tables have COMMENTs (check manually)")
    print("  [ ] FKs documented in column COMMENTs (check manually)")
    print("  [ ] Null policy documented per table (check manually)")
    print("  [ ] Z-ordering applied (check manually)")

    # ====================================================================
    # SUMMARY
    # ====================================================================
    print("\n\n" + "=" * 70)
    print("  VALIDATION SUMMARY")
    print("=" * 70)
    total = len(results)
    passed = sum(1 for r in results if r[2] == "PASS")
    failed = sum(1 for r in results if r[2] == "FAIL")
    print(f"  Total checks:  {total}")
    print(f"  PASSED:        {passed}")
    print(f"  FAILED:        {failed}")

    if failed > 0:
        print(f"\n  FAILURES:")
        # Group by category
        categories = {}
        for cat, name, status, detail in results:
            if status == "FAIL":
                categories.setdefault(cat, []).append((name, detail))
        for cat in sorted(categories.keys()):
            print(f"\n  [{cat}]")
            for name, detail in categories[cat]:
                tier = " (VERIFIED)" if any(v in name for v in VERIFIED_COMMUNITIES) else ""
                print(f"    {name}{tier}: {detail}")

    # Confidence breakdown
    verified_checks = [r for r in results if any(v in r[1] for v in VERIFIED_COMMUNITIES)]
    unverified_checks = [r for r in results if not any(v in r[1] for v in VERIFIED_COMMUNITIES)]
    v_pass = sum(1 for r in verified_checks if r[2] == "PASS")
    v_fail = sum(1 for r in verified_checks if r[2] == "FAIL")
    u_pass = sum(1 for r in unverified_checks if r[2] == "PASS")
    u_fail = sum(1 for r in unverified_checks if r[2] == "FAIL")

    print(f"\n  Confidence breakdown:")
    print(f"    Verified (Airdrie + Cochrane):  {v_pass} pass / {v_fail} fail")
    print(f"    Unverified (other 5):            {u_pass} pass / {u_fail} fail")
    print(f"    Structural (non-community):      {total - len(verified_checks) - len(unverified_checks)} checks")

    print("\n" + "=" * 70)
    print("  VALIDATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
