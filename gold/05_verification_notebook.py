# Databricks notebook source
# MAGIC %md
# MAGIC # MFAH Gold Layer - Data Verification Notebook
# MAGIC **Purpose:** Mirror every section of the [MFAH Regional Housing Dashboard](https://helpseekertechnologies.github.io/mfah-regional-housing/) using gold layer tables.
# MAGIC Compare each cell's output against the corresponding dashboard section to verify data accuracy.
# MAGIC
# MAGIC **Source:** `client_projects.mfah` (20 gold tables, 7 communities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Regional Overview
# MAGIC *Dashboard section: Overview - KPI cards + community selector*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI Overview: Population, Growth, Households, Income, Rent
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   d.pop_2021 AS `Population 2021`,
# MAGIC   d.pop_2016 AS `Population 2016`,
# MAGIC   d.growth_5yr_pct AS `5yr Growth %`,
# MAGIC   d.growth_10yr_pct AS `10yr Growth %`,
# MAGIC   d.households AS Households,
# MAGIC   d.single_detached AS `Single Detached`,
# MAGIC   d.tenant_pct AS `Tenant %`,
# MAGIC   d.median_income AS `Median Income`,
# MAGIC   d.median_rent AS `Median Rent`
# MAGIC FROM client_projects.mfah.hna_demographics_csd_2021 d
# MAGIC JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
# MAGIC ORDER BY d.pop_2021 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Age Distribution (compare against dashboard age pyramid)
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   d.age_0_14_pct AS `0-14 %`,
# MAGIC   d.age_15_24_pct AS `15-24 %`,
# MAGIC   d.age_25_44_pct AS `25-44 %`,
# MAGIC   d.age_45_64_pct AS `45-64 %`,
# MAGIC   d.age_65plus_pct AS `65+ %`
# MAGIC FROM client_projects.mfah.hna_demographics_csd_2021 d
# MAGIC JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
# MAGIC ORDER BY g.geography

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Core Housing Need
# MAGIC *Dashboard section: Housing Need - CHN KPIs, tenure comparison, housing indicators*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Core Housing Need: Total, Owner, Renter rates
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   h.chn_total AS `CHN Total`,
# MAGIC   h.chn_pct AS `CHN Rate %`,
# MAGIC   h.chn_owner AS `CHN Owner`,
# MAGIC   h.chn_owner_pct AS `Owner Rate %`,
# MAGIC   h.chn_renter AS `CHN Renter`,
# MAGIC   h.chn_renter_pct AS `Renter Rate %`
# MAGIC FROM client_projects.mfah.hna_housing_indicators_csd_2021 h
# MAGIC JOIN client_projects.mfah.geography_dim g ON h.geography_id = g.geography_id
# MAGIC ORDER BY h.chn_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Housing Indicators: Adequacy, Suitability, Affordability
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   h.inadequate_count AS Inadequate, h.inadequate_pct AS `Inad %`,
# MAGIC   h.unsuitable_count AS Unsuitable, h.unsuitable_pct AS `Unsuit %`,
# MAGIC   h.unaffordable_count AS Unaffordable, h.unaffordable_pct AS `Unaff %`,
# MAGIC   h.total_households AS `Total HH`
# MAGIC FROM client_projects.mfah.hna_housing_indicators_csd_2021 h
# MAGIC JOIN client_projects.mfah.geography_dim g ON h.geography_id = g.geography_id
# MAGIC ORDER BY g.geography

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CHN by Tenure: 2016 vs 2021 (chart: "CHN households by tenure")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   c.timeframe_id AS Year,
# MAGIC   c.chn_owner_count AS `Owner CHN`,
# MAGIC   c.chn_owner_rate AS `Owner Rate %`,
# MAGIC   c.chn_renter_count AS `Renter CHN`,
# MAGIC   c.chn_renter_rate AS `Renter Rate %`
# MAGIC FROM client_projects.mfah.hna_chn_tenure_csd_2016_2021 c
# MAGIC JOIN client_projects.mfah.geography_dim g ON c.geography_id = g.geography_id
# MAGIC ORDER BY g.geography, c.timeframe_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Affordability
# MAGIC *Dashboard section: Affordability - income categories, shelter costs, deficit*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HART 5-Tier Income Categories (chart: "% Households by income category")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   i.income_tier AS `Income Tier`,
# MAGIC   i.household_pct AS `HH %`,
# MAGIC   i.household_count AS `HH Count`,
# MAGIC   i.max_income AS `Max Income`,
# MAGIC   i.max_shelter_cost AS `Max Shelter`
# MAGIC FROM client_projects.mfah.hna_income_categories_csd_2021 i
# MAGIC JOIN client_projects.mfah.geography_dim g ON i.geography_id = g.geography_id
# MAGIC ORDER BY g.geography,
# MAGIC   CASE i.income_tier WHEN 'very_low' THEN 1 WHEN 'low' THEN 2 WHEN 'moderate' THEN 3 WHEN 'median' THEN 4 WHEN 'high' THEN 5 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Affordable Housing Deficit by Income Tier (chart: "Affordable housing deficit by income")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   d.income_tier AS `Income Tier`,
# MAGIC   SUM(d.deficit_count) AS `Total Deficit`,
# MAGIC   SUM(d.deficit_unsubsidized) AS Unsubsidized,
# MAGIC   SUM(d.deficit_subsidized) AS Subsidized
# MAGIC FROM client_projects.mfah.hna_housing_deficit_csd_2021 d
# MAGIC JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
# MAGIC GROUP BY g.geography, d.income_tier
# MAGIC HAVING SUM(d.deficit_count) > 0
# MAGIC ORDER BY g.geography,
# MAGIC   CASE d.income_tier WHEN 'very_low' THEN 1 WHEN 'low' THEN 2 WHEN 'moderate' THEN 3 WHEN 'median' THEN 4 WHEN 'high' THEN 5 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shelter Cost Distribution Historical (chart: "Shelter cost bands over time")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   s.timeframe_id AS Year,
# MAGIC   s.cost_band AS `Cost Band`,
# MAGIC   s.household_count AS `HH Count`
# MAGIC FROM client_projects.mfah.hna_shelter_cost_csd_2006_2021 s
# MAGIC JOIN client_projects.mfah.geography_dim g ON s.geography_id = g.geography_id
# MAGIC ORDER BY g.geography, s.timeframe_id,
# MAGIC   CASE s.cost_band WHEN 'under_500' THEN 1 WHEN 'r500_999' THEN 2 WHEN 'r1000_1499' THEN 3 WHEN 'r1500_1999' THEN 4 WHEN 'r2000_plus' THEN 5 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Income Distribution by Tenure (chart: "Income by tenure type")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   i.timeframe_id AS Year,
# MAGIC   i.tenure AS Tenure,
# MAGIC   i.income_band AS `Income Band`,
# MAGIC   i.household_count AS `HH Count`
# MAGIC FROM client_projects.mfah.hna_income_distribution_csd_2006_2021 i
# MAGIC JOIN client_projects.mfah.geography_dim g ON i.geography_id = g.geography_id
# MAGIC WHERE i.timeframe_id = 2021
# MAGIC ORDER BY g.geography, i.tenure,
# MAGIC   CASE i.income_band WHEN 'under_20k' THEN 1 WHEN 'r20k_40k' THEN 2 WHEN 'r40k_60k' THEN 3 WHEN 'r60k_80k' THEN 4 WHEN 'r80k_100k' THEN 5 WHEN 'over_100k' THEN 6 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Median Income Trends (chart: "Median income by tenure over time")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   m.timeframe_id AS Year,
# MAGIC   m.median_total AS `Median Total`,
# MAGIC   m.median_owner AS `Median Owner`,
# MAGIC   m.median_renter AS `Median Renter`,
# MAGIC   m.avg_total AS `Avg Total`,
# MAGIC   m.avg_owner AS `Avg Owner`,
# MAGIC   m.avg_renter AS `Avg Renter`
# MAGIC FROM client_projects.mfah.hna_median_income_csd_2006_2021 m
# MAGIC JOIN client_projects.mfah.geography_dim g ON m.geography_id = g.geography_id
# MAGIC ORDER BY g.geography, m.timeframe_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Housing Supply
# MAGIC *Dashboard section: Supply - rental composition, structure types, dwelling values*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rental Supply Composition (chart: "Primary vs secondary rental market")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   r.total_rental AS `Total Rental`,
# MAGIC   r.primary_rental AS `Primary`,
# MAGIC   r.primary_pct AS `Primary %`,
# MAGIC   r.secondary_rental AS `Secondary`,
# MAGIC   r.secondary_pct AS `Secondary %`,
# MAGIC   r.subsidized AS Subsidized,
# MAGIC   r.subsidized_pct AS `Subsid %`,
# MAGIC   r.below_market AS `Below Market`,
# MAGIC   r.co_op_units AS `Co-op`,
# MAGIC   r.affordable_built_2016_2021 AS Built,
# MAGIC   r.affordable_lost_2016_2021 AS Lost,
# MAGIC   r.affordable_net_change AS `Net Change`
# MAGIC FROM client_projects.mfah.hna_rental_supply_csd_2021 r
# MAGIC JOIN client_projects.mfah.geography_dim g ON r.geography_id = g.geography_id
# MAGIC ORDER BY r.total_rental DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dwelling Type Distribution (chart: "Dwelling type distribution")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   s.timeframe_id AS Year,
# MAGIC   s.structure_type AS `Type`,
# MAGIC   s.dwelling_count AS Count
# MAGIC FROM client_projects.mfah.hna_structure_type_csd_2006_2021 s
# MAGIC JOIN client_projects.mfah.geography_dim g ON s.geography_id = g.geography_id
# MAGIC WHERE s.timeframe_id = 2021
# MAGIC ORDER BY g.geography, s.dwelling_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average Dwelling Values by Type (chart: dwelling values over time)
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   d.timeframe_id AS Year,
# MAGIC   d.structure_type AS `Type`,
# MAGIC   d.avg_dwelling_value AS `Avg Value`
# MAGIC FROM client_projects.mfah.hna_dwelling_values_csd_2006_2021 d
# MAGIC JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
# MAGIC WHERE d.avg_dwelling_value > 0
# MAGIC   AND d.structure_type NOT IN ('other', 'high_rise')
# MAGIC ORDER BY g.geography, d.timeframe_id, d.avg_dwelling_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Growth Trends
# MAGIC *Dashboard section: Growth - population growth rates, structure type trends*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5-Year Population Growth Rate (chart: "5-year population growth rate")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   d.growth_5yr_pct AS `5yr Growth %`,
# MAGIC   d.pop_2016 AS `Pop 2016`,
# MAGIC   d.pop_2021 AS `Pop 2021`,
# MAGIC   d.pop_2021 - d.pop_2016 AS `Absolute Growth`
# MAGIC FROM client_projects.mfah.hna_demographics_csd_2021 d
# MAGIC JOIN client_projects.mfah.geography_dim g ON d.geography_id = g.geography_id
# MAGIC ORDER BY d.growth_5yr_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Structure Type Trends 2006-2021 (chart: "Dwelling type distribution" over time)
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   s.timeframe_id AS Year,
# MAGIC   s.structure_type AS `Structure Type`,
# MAGIC   s.dwelling_count AS Count
# MAGIC FROM client_projects.mfah.hna_structure_type_csd_2006_2021 s
# MAGIC JOIN client_projects.mfah.geography_dim g ON s.geography_id = g.geography_id
# MAGIC ORDER BY g.geography, s.timeframe_id, s.structure_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Economic Indicators
# MAGIC *Dashboard section: Economic - unemployment, EI, commuting, building permits*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Economic Snapshot (KPIs: unemployment, low income, sale price, commuter flow)
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   e.unemployment_rate_2021 AS `Unemployment %`,
# MAGIC   e.lim_all_pop_pct AS `Low Income (LIM) %`,
# MAGIC   e.lim_0_17_pct AS `LIM 0-17 %`,
# MAGIC   e.lim_18_64_pct AS `LIM 18-64 %`,
# MAGIC   e.lim_65plus_pct AS `LIM 65+ %`,
# MAGIC   e.avg_sale_price AS `Avg Sale Price`,
# MAGIC   e.owner_income AS `Owner Income`,
# MAGIC   e.renter_income AS `Renter Income`,
# MAGIC   e.net_commuter_flow_2021 AS `Net Commuter 2021`,
# MAGIC   e.net_commuter_flow_change_pct AS `Commuter Change %`
# MAGIC FROM client_projects.mfah.hna_economic_snapshot_csd_2021 e
# MAGIC JOIN client_projects.mfah.geography_dim g ON e.geography_id = g.geography_id
# MAGIC ORDER BY g.geography

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EI Recipients Time Series (chart: "EI recipients by year")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   e.timeframe_id AS Year,
# MAGIC   e.recipient_count AS `EI Recipients`
# MAGIC FROM client_projects.mfah.ei_recipients_csd_2016_2024 e
# MAGIC JOIN client_projects.mfah.geography_dim g ON e.geography_id = g.geography_id
# MAGIC ORDER BY g.geography, e.timeframe_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Building Permits by Sector (chart: "Residential building permits")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   b.timeframe_id AS Year,
# MAGIC   b.permit_sector AS Sector,
# MAGIC   b.permit_count AS `Permit Count`
# MAGIC FROM client_projects.mfah.hna_building_permits_csd_2016_2024 b
# MAGIC JOIN client_projects.mfah.geography_dim g ON b.geography_id = g.geography_id
# MAGIC ORDER BY g.geography, b.timeframe_id, b.permit_sector

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Rental Market
# MAGIC *Dashboard section: Rental Market - vacancy, rents, housing starts*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vacancy Rate Trends (chart: "Vacancy rate by community" + "Vacancy rate trend")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   r.timeframe_id AS Year,
# MAGIC   r.vacancy_rate AS `Vacancy Rate %`
# MAGIC FROM client_projects.mfah.cmhc_rental_market_csd_2016_2025 r
# MAGIC JOIN client_projects.mfah.geography_dim g ON r.geography_id = g.geography_id
# MAGIC WHERE r.vacancy_rate IS NOT NULL
# MAGIC ORDER BY g.geography, r.timeframe_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average Rent Trends (chart: "Average rent comparison" + "Average rent trend")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   r.timeframe_id AS Year,
# MAGIC   r.avg_rent AS `Avg Rent`,
# MAGIC   r.rent_yoy_change AS `YoY Change $`
# MAGIC FROM client_projects.mfah.cmhc_rental_market_csd_2016_2025 r
# MAGIC JOIN client_projects.mfah.geography_dim g ON r.geography_id = g.geography_id
# MAGIC WHERE r.avg_rent IS NOT NULL
# MAGIC ORDER BY g.geography, r.timeframe_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rent by Bedroom Type 2024 (KPIs in dashboard)
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   r.rent_bachelor AS Bachelor,
# MAGIC   r.rent_1bed AS `1-Bed`,
# MAGIC   r.rent_2bed AS `2-Bed`,
# MAGIC   r.rent_3bed AS `3-Bed`
# MAGIC FROM client_projects.mfah.cmhc_rental_market_csd_2016_2025 r
# MAGIC JOIN client_projects.mfah.geography_dim g ON r.geography_id = g.geography_id
# MAGIC WHERE r.timeframe_id = 2024
# MAGIC   AND (r.rent_1bed IS NOT NULL OR r.rent_3bed IS NOT NULL)
# MAGIC ORDER BY g.geography

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Housing Starts by Structure Type (chart: "Housing starts by community")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   h.timeframe_id AS Year,
# MAGIC   h.category AS `Structure Type`,
# MAGIC   h.starts_count AS Starts
# MAGIC FROM client_projects.mfah.cmhc_housing_starts_csd_2016_2024 h
# MAGIC JOIN client_projects.mfah.geography_dim g ON h.geography_id = g.geography_id
# MAGIC WHERE h.dimension_type = 'structure_type'
# MAGIC ORDER BY g.geography, h.timeframe_id, h.category

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Housing Starts by Tenure (Owner vs Rental vs Condo)
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   h.timeframe_id AS Year,
# MAGIC   h.category AS Tenure,
# MAGIC   h.starts_count AS Starts
# MAGIC FROM client_projects.mfah.cmhc_housing_starts_csd_2016_2024 h
# MAGIC JOIN client_projects.mfah.geography_dim g ON h.geography_id = g.geography_id
# MAGIC WHERE h.dimension_type = 'tenure'
# MAGIC ORDER BY g.geography, h.timeframe_id, h.category

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Priority Populations
# MAGIC *Dashboard section: Priority - CHN rates by vulnerable group*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Priority Groups: CHN by Population Subgroup (chart: "CHN rate by priority group")
# MAGIC SELECT
# MAGIC   g.geography AS Community,
# MAGIC   p.priority_group AS `Priority Group`,
# MAGIC   p.chn_count AS `CHN Count`,
# MAGIC   p.chn_rate AS `CHN Rate %`
# MAGIC FROM client_projects.mfah.hna_priority_groups_csd_2021 p
# MAGIC JOIN client_projects.mfah.geography_dim g ON p.geography_id = g.geography_id
# MAGIC ORDER BY g.geography, p.chn_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Summary
# MAGIC *Cross-checks and completeness - not in the dashboard, but validates the gold layer*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts per table
# MAGIC SELECT 'geography_dim' AS `Table`, COUNT(*) AS Rows FROM client_projects.mfah.geography_dim
# MAGIC UNION ALL SELECT 'timeframe_dim', COUNT(*) FROM client_projects.mfah.timeframe_dim
# MAGIC UNION ALL SELECT 'source_dim', COUNT(*) FROM client_projects.mfah.source_dim
# MAGIC UNION ALL SELECT 'hna_demographics_csd_2021', COUNT(*) FROM client_projects.mfah.hna_demographics_csd_2021
# MAGIC UNION ALL SELECT 'hna_housing_indicators_csd_2021', COUNT(*) FROM client_projects.mfah.hna_housing_indicators_csd_2021
# MAGIC UNION ALL SELECT 'hna_income_categories_csd_2021', COUNT(*) FROM client_projects.mfah.hna_income_categories_csd_2021
# MAGIC UNION ALL SELECT 'hna_housing_deficit_csd_2021', COUNT(*) FROM client_projects.mfah.hna_housing_deficit_csd_2021
# MAGIC UNION ALL SELECT 'hna_rental_supply_csd_2021', COUNT(*) FROM client_projects.mfah.hna_rental_supply_csd_2021
# MAGIC UNION ALL SELECT 'hna_priority_groups_csd_2021', COUNT(*) FROM client_projects.mfah.hna_priority_groups_csd_2021
# MAGIC UNION ALL SELECT 'hna_economic_snapshot_csd_2021', COUNT(*) FROM client_projects.mfah.hna_economic_snapshot_csd_2021
# MAGIC UNION ALL SELECT 'hna_structure_type_csd_2006_2021', COUNT(*) FROM client_projects.mfah.hna_structure_type_csd_2006_2021
# MAGIC UNION ALL SELECT 'hna_shelter_cost_csd_2006_2021', COUNT(*) FROM client_projects.mfah.hna_shelter_cost_csd_2006_2021
# MAGIC UNION ALL SELECT 'hna_income_distribution_csd_2006_2021', COUNT(*) FROM client_projects.mfah.hna_income_distribution_csd_2006_2021
# MAGIC UNION ALL SELECT 'hna_dwelling_values_csd_2006_2021', COUNT(*) FROM client_projects.mfah.hna_dwelling_values_csd_2006_2021
# MAGIC UNION ALL SELECT 'hna_median_income_csd_2006_2021', COUNT(*) FROM client_projects.mfah.hna_median_income_csd_2006_2021
# MAGIC UNION ALL SELECT 'cmhc_rental_market_csd_2016_2025', COUNT(*) FROM client_projects.mfah.cmhc_rental_market_csd_2016_2025
# MAGIC UNION ALL SELECT 'cmhc_housing_starts_csd_2016_2024', COUNT(*) FROM client_projects.mfah.cmhc_housing_starts_csd_2016_2024
# MAGIC UNION ALL SELECT 'hna_chn_tenure_csd_2016_2021', COUNT(*) FROM client_projects.mfah.hna_chn_tenure_csd_2016_2021
# MAGIC UNION ALL SELECT 'ei_recipients_csd_2016_2024', COUNT(*) FROM client_projects.mfah.ei_recipients_csd_2016_2024
# MAGIC UNION ALL SELECT 'hna_building_permits_csd_2016_2024', COUNT(*) FROM client_projects.mfah.hna_building_permits_csd_2016_2024
# MAGIC ORDER BY `Table`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FK Integrity Check: any orphaned geography references?
# MAGIC SELECT 'demographics' AS tbl, COUNT(*) AS orphans FROM client_projects.mfah.hna_demographics_csd_2021 f LEFT JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id WHERE g.geography_id IS NULL
# MAGIC UNION ALL SELECT 'housing_indicators', COUNT(*) FROM client_projects.mfah.hna_housing_indicators_csd_2021 f LEFT JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id WHERE g.geography_id IS NULL
# MAGIC UNION ALL SELECT 'ei_recipients', COUNT(*) FROM client_projects.mfah.ei_recipients_csd_2016_2024 f LEFT JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id WHERE g.geography_id IS NULL
# MAGIC UNION ALL SELECT 'rental_market', COUNT(*) FROM client_projects.mfah.cmhc_rental_market_csd_2016_2025 f LEFT JOIN client_projects.mfah.geography_dim g ON f.geography_id = g.geography_id WHERE g.geography_id IS NULL
