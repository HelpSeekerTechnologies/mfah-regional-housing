-- ============================================================================
-- MFAH Gold Layer — Databricks SQL Dashboard Queries
-- Use these in Databricks SQL > SQL Editor or Dashboard to visually verify
-- that the gold layer data matches the HTML dashboard.
-- ============================================================================

-- ============================================================================
-- 1. REGIONAL OVERVIEW: Population, Growth, Households
-- Chart: Bar chart (pop_2021 by community)
-- ============================================================================
SELECT
  g.geography AS community,
  d.pop_2021 AS population_2021,
  d.pop_2016 AS population_2016,
  d.growth_5yr_pct AS growth_pct,
  d.households,
  d.median_income,
  d.median_rent,
  d.tenant_pct
FROM client_projects.mfah.hna_demographics_csd_2021 d
JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
ORDER BY d.pop_2021 DESC;

-- ============================================================================
-- 2. CORE HOUSING NEED: CHN Rate by Community
-- Chart: Horizontal bar chart (chn_pct by community)
-- ============================================================================
SELECT
  g.geography AS community,
  h.chn_total,
  h.chn_pct AS chn_rate,
  h.chn_owner_pct AS owner_rate,
  h.chn_renter_pct AS renter_rate,
  h.inadequate_count,
  h.unsuitable_count,
  h.unaffordable_count
FROM client_projects.mfah.hna_housing_indicators_csd_2021 h
JOIN client_projects.mfah.geography_dim g ON h.geography_id = g.geography_id
ORDER BY h.chn_pct DESC;

-- ============================================================================
-- 3. AFFORDABILITY: Income Categories (HART 5-tier)
-- Chart: Stacked bar (household_count by income_tier, grouped by community)
-- ============================================================================
SELECT
  g.geography AS community,
  i.income_tier,
  i.household_pct,
  i.household_count,
  i.max_shelter_cost
FROM client_projects.mfah.hna_income_categories_csd_2021 i
JOIN client_projects.mfah.geography_dim g ON i.geography_id = g.geography_id
ORDER BY g.geography,
  CASE i.income_tier
    WHEN 'very_low' THEN 1
    WHEN 'low' THEN 2
    WHEN 'moderate' THEN 3
    WHEN 'median' THEN 4
    WHEN 'high' THEN 5
  END;

-- ============================================================================
-- 4. HOUSING SUPPLY: Rental Composition
-- Chart: Stacked bar (primary vs secondary rental by community)
-- ============================================================================
SELECT
  g.geography AS community,
  r.total_rental,
  r.primary_rental,
  r.secondary_rental,
  r.primary_pct,
  r.subsidized,
  r.subsidized_pct,
  r.below_market,
  r.co_op_units,
  r.affordable_net_change
FROM client_projects.mfah.hna_rental_supply_csd_2021 r
JOIN client_projects.mfah.geography_dim g ON r.geography_id = g.geography_id
ORDER BY r.total_rental DESC;

-- ============================================================================
-- 5. GROWTH TRENDS: Structure Type Over Time (Airdrie + Cochrane)
-- Chart: Stacked area (dwelling_count by structure_type over time)
-- ============================================================================
SELECT
  g.geography AS community,
  s.timeframe_id AS year,
  s.structure_type,
  s.dwelling_count
FROM client_projects.mfah.hna_structure_type_csd_2006_2021 s
JOIN client_projects.mfah.geography_dim g ON s.geography_id = g.geography_id
ORDER BY g.geography, s.timeframe_id, s.structure_type;

-- ============================================================================
-- 6. ECONOMIC INDICATORS: Unemployment + Low Income
-- Chart: Table or multi-metric bar
-- ============================================================================
SELECT
  g.geography AS community,
  e.unemployment_rate_2021,
  e.lim_all_pop_pct AS low_income_pct,
  e.avg_sale_price,
  e.owner_income,
  e.renter_income,
  e.net_commuter_flow_2021,
  e.net_commuter_flow_change_pct
FROM client_projects.mfah.hna_economic_snapshot_csd_2021 e
JOIN client_projects.mfah.geography_dim g ON e.geography_id = g.geography_id
ORDER BY e.unemployment_rate_2021 DESC;

-- ============================================================================
-- 7. RENTAL MARKET: Vacancy Rate Trends (All 7 communities)
-- Chart: Line chart (vacancy_rate by year, one line per community)
-- ============================================================================
SELECT
  g.geography AS community,
  r.timeframe_id AS year,
  r.vacancy_rate,
  r.avg_rent,
  r.rent_yoy_change
FROM client_projects.mfah.cmhc_rental_market_csd_2016_2025 r
JOIN client_projects.mfah.geography_dim g ON r.geography_id = g.geography_id
WHERE r.vacancy_rate IS NOT NULL
ORDER BY g.geography, r.timeframe_id;

-- ============================================================================
-- 8. RENTAL MARKET: Average Rent Trends
-- Chart: Line chart (avg_rent by year, one line per community)
-- ============================================================================
SELECT
  g.geography AS community,
  r.timeframe_id AS year,
  r.avg_rent
FROM client_projects.mfah.cmhc_rental_market_csd_2016_2025 r
JOIN client_projects.mfah.geography_dim g ON r.geography_id = g.geography_id
WHERE r.avg_rent IS NOT NULL
ORDER BY g.geography, r.timeframe_id;

-- ============================================================================
-- 9. HOUSING STARTS: Total by Structure Type
-- Chart: Stacked bar (starts_count by category, grouped by year)
-- ============================================================================
SELECT
  g.geography AS community,
  h.timeframe_id AS year,
  h.category,
  h.starts_count
FROM client_projects.mfah.cmhc_housing_starts_csd_2016_2024 h
JOIN client_projects.mfah.geography_dim g ON h.geography_id = g.geography_id
WHERE h.dimension_type = 'structure_type'
  AND g.geography_key = 'airdrie'
ORDER BY h.timeframe_id, h.category;

-- ============================================================================
-- 10. HOUSING STARTS: Tenure Split (Owner vs Rental vs Condo)
-- Chart: Stacked bar
-- ============================================================================
SELECT
  g.geography AS community,
  h.timeframe_id AS year,
  h.category AS tenure,
  h.starts_count
FROM client_projects.mfah.cmhc_housing_starts_csd_2016_2024 h
JOIN client_projects.mfah.geography_dim g ON h.geography_id = g.geography_id
WHERE h.dimension_type = 'tenure'
  AND g.geography_key = 'airdrie'
ORDER BY h.timeframe_id, h.category;

-- ============================================================================
-- 11. EI RECIPIENTS: Time Series (All Communities)
-- Chart: Line chart (recipient_count by year, one line per community)
-- ============================================================================
SELECT
  g.geography AS community,
  e.timeframe_id AS year,
  e.recipient_count
FROM client_projects.mfah.ei_recipients_csd_2016_2024 e
JOIN client_projects.mfah.geography_dim g ON e.geography_id = g.geography_id
ORDER BY g.geography, e.timeframe_id;

-- ============================================================================
-- 12. PRIORITY POPULATIONS: CHN by Group (Airdrie)
-- Chart: Horizontal bar (chn_count by priority_group)
-- ============================================================================
SELECT
  p.priority_group,
  p.chn_count,
  p.chn_rate
FROM client_projects.mfah.hna_priority_groups_csd_2021 p
JOIN client_projects.mfah.geography_dim g ON p.geography_id = g.geography_id
WHERE g.geography_key = 'airdrie'
  AND p.priority_group != 'community_total'
ORDER BY p.chn_count DESC;

-- ============================================================================
-- 13. SHELTER COST DISTRIBUTION: Historical (Airdrie)
-- Chart: Stacked bar (household_count by cost_band, grouped by year)
-- ============================================================================
SELECT
  s.timeframe_id AS year,
  s.cost_band,
  s.household_count
FROM client_projects.mfah.hna_shelter_cost_csd_2006_2021 s
JOIN client_projects.mfah.geography_dim g ON s.geography_id = g.geography_id
WHERE g.geography_key = 'airdrie'
ORDER BY s.timeframe_id,
  CASE s.cost_band
    WHEN 'under_500' THEN 1
    WHEN 'r500_999' THEN 2
    WHEN 'r1000_1499' THEN 3
    WHEN 'r1500_1999' THEN 4
    WHEN 'r2000_plus' THEN 5
  END;

-- ============================================================================
-- 14. INCOME DISTRIBUTION: Historical by Tenure (Airdrie)
-- Chart: Grouped bar (household_count by income_band for each year)
-- ============================================================================
SELECT
  i.timeframe_id AS year,
  i.tenure,
  i.income_band,
  i.household_count
FROM client_projects.mfah.hna_income_distribution_csd_2006_2021 i
JOIN client_projects.mfah.geography_dim g ON i.geography_id = g.geography_id
WHERE g.geography_key = 'airdrie'
  AND i.tenure = 'total'
ORDER BY i.timeframe_id,
  CASE i.income_band
    WHEN 'under_20k' THEN 1
    WHEN 'r20k_40k' THEN 2
    WHEN 'r40k_60k' THEN 3
    WHEN 'r60k_80k' THEN 4
    WHEN 'r80k_100k' THEN 5
    WHEN 'over_100k' THEN 6
  END;

-- ============================================================================
-- 15. MEDIAN INCOME TRENDS (Airdrie + Cochrane)
-- Chart: Line chart (median_income by year, by tenure)
-- ============================================================================
SELECT
  g.geography AS community,
  m.timeframe_id AS year,
  m.median_total,
  m.median_owner,
  m.median_renter,
  m.avg_total,
  m.avg_owner,
  m.avg_renter
FROM client_projects.mfah.hna_median_income_csd_2006_2021 m
JOIN client_projects.mfah.geography_dim g ON m.geography_id = g.geography_id
ORDER BY g.geography, m.timeframe_id;

-- ============================================================================
-- 16. DWELLING VALUES: Historical by Type (Airdrie)
-- Chart: Line or grouped bar
-- ============================================================================
SELECT
  d.timeframe_id AS year,
  d.structure_type,
  d.avg_dwelling_value
FROM client_projects.mfah.hna_dwelling_values_csd_2006_2021 d
JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
WHERE g.geography_key = 'airdrie'
  AND d.structure_type NOT IN ('other', 'high_rise')
  AND d.avg_dwelling_value > 0
ORDER BY d.timeframe_id, d.structure_type;

-- ============================================================================
-- 17. CHN BY TENURE: 2016 vs 2021
-- Chart: Grouped bar (owner vs renter, 2016 vs 2021)
-- ============================================================================
SELECT
  g.geography AS community,
  c.timeframe_id AS year,
  c.chn_owner_count,
  c.chn_owner_rate,
  c.chn_renter_count,
  c.chn_renter_rate
FROM client_projects.mfah.hna_chn_tenure_csd_2016_2021 c
JOIN client_projects.mfah.geography_dim g ON c.geography_id = g.geography_id
ORDER BY g.geography, c.timeframe_id;

-- ============================================================================
-- 18. BUILDING PERMITS: By Sector
-- Chart: Stacked bar (permit_count by sector, grouped by year)
-- ============================================================================
SELECT
  g.geography AS community,
  b.timeframe_id AS year,
  b.permit_sector,
  b.permit_count
FROM client_projects.mfah.hna_building_permits_csd_2016_2024 b
JOIN client_projects.mfah.geography_dim g ON b.geography_id = g.geography_id
WHERE g.geography_key = 'airdrie'
ORDER BY b.timeframe_id, b.permit_sector;

-- ============================================================================
-- 19. HOUSING DEFICIT: By Income Tier and Household Size (Airdrie)
-- Chart: Heatmap or pivot table
-- ============================================================================
SELECT
  d.income_tier,
  d.household_size,
  d.deficit_count,
  d.deficit_unsubsidized,
  d.deficit_subsidized
FROM client_projects.mfah.hna_housing_deficit_csd_2021 d
JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
WHERE g.geography_key = 'airdrie'
  AND d.deficit_count > 0
ORDER BY
  CASE d.income_tier
    WHEN 'very_low' THEN 1
    WHEN 'low' THEN 2
    WHEN 'moderate' THEN 3
    WHEN 'median' THEN 4
    WHEN 'high' THEN 5
  END,
  d.household_size;

-- ============================================================================
-- 20. DATA COMPLETENESS SUMMARY
-- Chart: Table — shows which communities have data in each table
-- ============================================================================
SELECT 'hna_demographics_csd_2021' AS table_name, g.geography AS community, COUNT(*) AS rows
FROM client_projects.mfah.hna_demographics_csd_2021 f
JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id
GROUP BY g.geography
UNION ALL
SELECT 'hna_structure_type_csd_2006_2021', g.geography, COUNT(*)
FROM client_projects.mfah.hna_structure_type_csd_2006_2021 f
JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id
GROUP BY g.geography
UNION ALL
SELECT 'ei_recipients_csd_2016_2024', g.geography, COUNT(*)
FROM client_projects.mfah.ei_recipients_csd_2016_2024 f
JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id
GROUP BY g.geography
UNION ALL
SELECT 'cmhc_housing_starts_csd_2016_2024', g.geography, COUNT(*)
FROM client_projects.mfah.cmhc_housing_starts_csd_2016_2024 f
JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id
GROUP BY g.geography
UNION ALL
SELECT 'hna_priority_groups_csd_2021', g.geography, COUNT(*)
FROM client_projects.mfah.hna_priority_groups_csd_2021 f
JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id
GROUP BY g.geography
ORDER BY table_name, community;
