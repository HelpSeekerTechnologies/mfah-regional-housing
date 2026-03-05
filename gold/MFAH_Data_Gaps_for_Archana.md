# MFAH Dashboard - Data Gaps & Issues for Archana

**Date:** 2026-03-05
**Scope:** 7 communities in `public_data.housing.hna_dashboard_2006_2031`
**Communities:** Airdrie (4806021), Cochrane (4806019), Okotoks (4806012), Chestermere (4806017), Strathmore (4805018), Rocky View County (4806014), Crossfield (4806026)

---

## MISSING DATA

### 1. Census 2016 CSD profile -- NOT IN public_data.housing
- `census_csd_2016_raw` exists in `public_data.census` but no cleaned/silver version
- `hna_dashboard` only has 2006 and 2011 for demographics (not 2016)
- **Needed for:** Population growth 2016->2021, historical income/dwelling trends, shelter cost distribution by year
- **Impact:** Can't show 4-point time series (2006/2011/2016/2021) -- only 2006/2011 available

### 2. Housing Indicators (adequacy/suitability/affordability breakdown) -- NOT IN hna_dashboard
- `core_housing_need` section has CHN totals and tenure splits but NOT:
  - Inadequate housing count/rate (by owner/renter)
  - Unsuitable housing count/rate (by owner/renter)
  - Unaffordable housing count/rate (by owner/renter)
  - % spending 30%+ on shelter costs
- **Available in:** `public_data.census.census_csd_2021` (characteristics: "Not suitable", "Spending 30% or more of income on shelter costs", "% in core housing need")
- **Action:** Add to `hna_dashboard` under `core_housing_need` section, or create a new view joining Census

### 3. Commuter Flow (net commuter flow) -- NOT IN hna_dashboard
- Census 2021 has commuting characteristics ("Total - Commuting destination...", "Usual place of work") but not net flow calculations
- **Needed for:** Economic section showing net commuter flow 2011/2016/2021 and % change
- **Impact:** Cannot replicate commuter flow comparison chart

### 4. Family Income by Type -- NOT IN hna_dashboard
- Dashboard shows income by couple/lone-parent/non-family households
- **May be in:** Census 2021 (needs characteristic mapping)
- **Impact:** Cannot replicate family income chart

---

## INCOMPLETE DATA (partial community coverage)

### 5. HART Rent History -- 4 of 7 communities only
- **Have data:** Airdrie (2016-2023), Cochrane (2016-2022), Okotoks (2016-2023), Strathmore (2016-2023)
- **Missing:** Chestermere (4806017), Rocky View County (4806014), Crossfield (4806026)
- **Table:** `public_data.housing.hart_avg_rent_history`

### 6. Transit Accessibility -- 4 of 7 communities only
- **Have data:** Airdrie, Cochrane, Okotoks, Strathmore
- **Missing:** Chestermere (4806017), Rocky View County (4806014), Crossfield (4806026)
- **Table:** `public_data.housing.hart_transit_accessibility`

### 7. HART Income Distribution -- essentially empty for MFAH
- Only 1 row for Okotoks in `hart_income_dist` (602 rows total, mostly non-MFAH)
- **Needed for:** Co-op housing units, income distribution details
- **Table:** `public_data.housing.hart_income_dist`

### 8. CMHC Rent by Bedroom -- uneven coverage
- **Good:** Airdrie (all bedroom types, 17-21 years), Cochrane (12-21 years), Strathmore (18 years)
- **Sparse:** Okotoks (missing Studio + 1-Bed for most years), Chestermere (only 1 row per type)
- **Missing entirely:** Rocky View County, Crossfield
- **In hna_dashboard under:** `rental_market` section

---

## DATA QUALITY ISSUES

### 9. Demographics timeframe anomaly
- `hna_dashboard` demographics section has timeframes: `2006`, `2011`, `Percentage of all HHs`
- The third value should be a year (likely 2021) not a string
- **Table:** `hna_dashboard_2006_2031` WHERE `dashboard_section = 'demographics'`

### 10. HART geography names are messy (but IDs are correct)
- `hart_priority_populations` has correct names (Airdrie, Cochrane, etc.)
- But other HART tables have wrong names for correct IDs:
  - `hart_chn_tenure_counts`: Cochrane/Okotoks/Crossfield show as "Unknown"
  - `hart_chn_income_hhsize`: names have appended stats like "Airdrie CY (4806021) 00000 (  1.8%)"
  - `hart_amhi`: names include type like "Airdrie CY (CSD, AL)"
- **Impact:** JOIN by geography_id works fine, but display names need cleaning
- **Suggestion:** Standardize geography names in silver layer using `hna_geography_dim` as lookup

---

## STALE / NEEDS REFRESH

### 11. Census 2006/2011 data is very limited
- Only 686 rows for 7 communities (49 characteristics x 2 years x 7 communities)
- Missing many characteristics available in Census 2021 (18,417 rows)
- `census_csd_2006_2011` has: Population, Households, Dwellings, Income, Age, Education, Employment, Mobility, Race, Family
- Does NOT have: Dwelling types by structure, shelter costs, commuting, housing tenure details

### 12. HART Cochrane rent history ends at 2022
- Other communities go to 2023, Cochrane stops at 2022
- **Table:** `hart_avg_rent_history` WHERE geography_id = '4806019'

---

## SUMMARY TABLE

| # | Issue | Type | Priority | Dashboard Section Affected |
|---|-------|------|----------|---------------------------|
| 1 | Census 2016 CSD profile | MISSING | High | Demographics, Growth, Affordability |
| 2 | Housing adequacy/suitability breakdown | MISSING | High | Core Housing Need |
| 3 | Commuter flow | MISSING | Medium | Economic |
| 4 | Family income by type | MISSING | Medium | Economic |
| 5 | HART rent history (3 communities) | INCOMPLETE | Medium | Rental Market |
| 6 | Transit accessibility (3 communities) | INCOMPLETE | Low | Demographics |
| 7 | HART income distribution | INCOMPLETE | Low | Affordability |
| 8 | CMHC rent by bedroom (2 communities) | INCOMPLETE | Medium | Rental Market |
| 9 | Demographics timeframe = "Percentage of all HHs" | DATA QUALITY | High | Demographics |
| 10 | HART geography name inconsistencies | DATA QUALITY | Medium | All HART sections |
| 11 | Census 2006/2011 limited characteristics | STALE | Low | Growth trends |
| 12 | Cochrane rent history ends 2022 | STALE | Low | Rental Market |
