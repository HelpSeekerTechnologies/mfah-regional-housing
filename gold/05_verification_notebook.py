# Databricks notebook source
# MAGIC %md
# MAGIC # MFAH Regional Housing Dashboard - Data Verification (v2)
# MAGIC **Source:** `public_data.housing.hna_dashboard_2006_2031` (6,240 rows, 7 communities, 8 sections)
# MAGIC
# MAGIC This notebook replicates every section of the MFAH Regional Housing Dashboard using the unified dashboard table.
# MAGIC Each section includes SQL queries and matplotlib charts styled with HelpSeeker brand colors.
# MAGIC
# MAGIC **Communities:** Airdrie, Cochrane, Okotoks, Chestermere, Strathmore, Rocky View County, Crossfield

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Brand Colors and Chart Helpers

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# HelpSeeker brand palette
SLATE = '#1E3A5F'
TEAL = '#0FB9B1'
AQUA = '#4FD1C5'
NAVY = '#0B1F33'
OCEAN = '#2C5282'
LIGHT_AQUA = '#7EDDD5'
PALE_AQUA = '#E0F0EE'
MIST = '#EDF5F4'

PALETTE = [SLATE, TEAL, AQUA, NAVY, OCEAN, LIGHT_AQUA, PALE_AQUA]
PALETTE_SHORT = [SLATE, TEAL, AQUA, NAVY, OCEAN]

def style_ax(ax, title, xlabel=None, ylabel=None):
    """Apply HelpSeeker styling to a matplotlib axes."""
    ax.set_title(title, fontsize=14, fontweight='bold', color=NAVY, pad=12)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=10, color=NAVY)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=10, color=NAVY)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#D1D5DB')
    ax.spines['bottom'].set_color('#D1D5DB')
    ax.tick_params(colors='#4A5568', labelsize=9)
    ax.set_facecolor('white')
    return ax

def fmt_number(val):
    """Format number with commas."""
    try:
        return f"{int(val):,}"
    except (ValueError, TypeError):
        return str(val)

print("Setup complete. Brand colors and helpers loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Data Completeness Overview
# MAGIC Quick scan of what is available in the dashboard table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts by dashboard section
# MAGIC SELECT
# MAGIC   dashboard_section,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   COUNT(DISTINCT geography) AS communities,
# MAGIC   COUNT(DISTINCT characteristic) AS characteristics,
# MAGIC   COUNT(DISTINCT timeframe) AS timeframes
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC GROUP BY dashboard_section
# MAGIC ORDER BY dashboard_section

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full list of communities
# MAGIC SELECT DISTINCT geography, geography_id, geography_type
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC ORDER BY geography

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Demographics
# MAGIC *Population, households, dwelling types, income, age, and labour force indicators from Census 2006 and 2011.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demographics: All characteristics and timeframes
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value,
# MAGIC   data_source
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'demographics'
# MAGIC ORDER BY geography, characteristic, timeframe

# COMMAND ----------

# Chart 1.1: Population by Community (2011)
df = spark.sql("""
  SELECT geography, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'demographics'
    AND characteristic = 'Population'
    AND timeframe = '2011'
  ORDER BY indicator_value DESC
""").toPandas()

if len(df) > 0:
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(df['geography'], df['indicator_value'].astype(float), color=SLATE, height=0.6)
    style_ax(ax, 'Population by Community (Census 2011)')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    ax.invert_yaxis()
    for bar in bars:
        w = bar.get_width()
        ax.text(w + w * 0.01, bar.get_y() + bar.get_height()/2, f'{w:,.0f}',
                va='center', fontsize=9, color='#4A5568')
    plt.tight_layout()
    plt.show()
else:
    print("No population data found for 2011.")

# COMMAND ----------

# Chart 1.2: Population Comparison 2006 vs 2011
df = spark.sql("""
  SELECT geography, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'demographics'
    AND characteristic = 'Population'
    AND timeframe IN ('2006', '2011')
  ORDER BY geography, timeframe
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='timeframe', values='indicator_value').fillna(0)
    pivot = pivot.sort_values(by=pivot.columns[-1], ascending=True)

    fig, ax = plt.subplots(figsize=(10, 5))
    y = np.arange(len(pivot))
    h = 0.35
    cols = sorted(pivot.columns)
    colors = [AQUA, SLATE]
    for i, col in enumerate(cols):
        ax.barh(y + i * h, pivot[col], height=h, label=col, color=colors[i % len(colors)])
    ax.set_yticks(y + h / 2)
    ax.set_yticklabels(pivot.index)
    style_ax(ax, 'Population Growth: 2006 vs 2011')
    ax.legend(frameon=False)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No population data found for 2006/2011.")

# COMMAND ----------

# Chart 1.3: Dwelling Type Stacked Bar (2011)
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'demographics'
    AND characteristic LIKE 'Dwelling:%'
    AND timeframe = '2011'
  ORDER BY geography, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    bottom = np.zeros(len(pivot))
    for i, col in enumerate(pivot.columns):
        ax.barh(pivot.index, pivot[col], left=bottom, label=col.replace('Dwelling: ', ''),
                color=PALETTE[i % len(PALETTE)], height=0.6)
        bottom += pivot[col].values
    style_ax(ax, 'Dwelling Types by Community (Census 2011)')
    ax.legend(loc='lower right', fontsize=8, frameon=False)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No dwelling type data found for 2011.")

# COMMAND ----------

# Chart 1.4: Demographics Comparison Table
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'demographics'
    AND characteristic IN ('Population', 'Households', 'Median Household Income',
                           'Median Age', 'Employment Rate (%)', 'Unemployment Rate (%)')
    AND timeframe = '2011'
  ORDER BY characteristic, geography
""").toPandas()

if len(df) > 0:
    pivot = df.pivot(index='characteristic', columns='geography', values='indicator_value')
    display(pivot)
else:
    print("No demographic comparison data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Core Housing Need
# MAGIC *CHN rates by community, tenure (owner vs renter), and income level. Census 2016 and 2021.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Core Housing Need: All characteristics
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'core_housing_need'
# MAGIC ORDER BY geography, timeframe, characteristic

# COMMAND ----------

# Chart 2.1: CHN Rate by Community (2021)
df = spark.sql("""
  SELECT geography, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'core_housing_need'
    AND characteristic = 'CHN rate'
    AND timeframe = '2021'
  ORDER BY indicator_value DESC
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(df['geography'], df['indicator_value'], color=SLATE, height=0.6)
    style_ax(ax, 'Core Housing Need Rate by Community (2021)')
    ax.set_xlabel('CHN Rate (%)')
    ax.invert_yaxis()
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 0.2, bar.get_y() + bar.get_height()/2, f'{w:.1f}%',
                va='center', fontsize=9, color='#4A5568')
    plt.tight_layout()
    plt.show()
else:
    print("No CHN rate data found for 2021.")

# COMMAND ----------

# Chart 2.2: Owner vs Renter CHN Rate (2021)
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'core_housing_need'
    AND characteristic IN ('Owner (rate)', 'Renter (rate)')
    AND timeframe = '2021'
  ORDER BY geography
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(pivot))
    w = 0.35
    if 'Owner (rate)' in pivot.columns:
        ax.bar(x - w/2, pivot['Owner (rate)'], w, label='Owner', color=SLATE)
    if 'Renter (rate)' in pivot.columns:
        ax.bar(x + w/2, pivot['Renter (rate)'], w, label='Renter', color=TEAL)
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'CHN Rate: Owner vs Renter (2021)', ylabel='CHN Rate (%)')
    ax.legend(frameon=False)
    plt.tight_layout()
    plt.show()
else:
    print("No owner/renter CHN rate data found.")

# COMMAND ----------

# Chart 2.3: CHN by Tenure - 2016 vs 2021
df = spark.sql("""
  SELECT geography, timeframe, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'core_housing_need'
    AND characteristic = 'CHN rate'
    AND timeframe IN ('2016', '2021')
  ORDER BY geography, timeframe
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='timeframe', values='indicator_value').fillna(0)
    pivot = pivot.sort_values(by='2021', ascending=True)

    fig, ax = plt.subplots(figsize=(10, 5))
    y = np.arange(len(pivot))
    h = 0.35
    if '2016' in pivot.columns:
        ax.barh(y - h/2, pivot['2016'], h, label='2016', color=AQUA)
    if '2021' in pivot.columns:
        ax.barh(y + h/2, pivot['2021'], h, label='2021', color=SLATE)
    ax.set_yticks(y)
    ax.set_yticklabels(pivot.index)
    style_ax(ax, 'CHN Rate Change: 2016 vs 2021')
    ax.set_xlabel('CHN Rate (%)')
    ax.legend(frameon=False)
    plt.tight_layout()
    plt.show()
else:
    print("No CHN rate data found for 2016/2021 comparison.")

# COMMAND ----------

# Chart 2.4: CHN by Income Level (2021)
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'core_housing_need'
    AND characteristic LIKE '%income HH in CHN'
    AND timeframe = '2021'
  ORDER BY geography, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(pivot))
    n_cols = len(pivot.columns)
    w = 0.8 / max(n_cols, 1)
    for i, col in enumerate(pivot.columns):
        short_label = col.replace(' income HH in CHN', '').replace(' HH in CHN', '')
        ax.bar(x + i * w - (n_cols - 1) * w / 2, pivot[col], w,
               label=short_label, color=PALETTE[i % len(PALETTE)])
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Households in CHN by Income Level (2021)', ylabel='Households')
    ax.legend(fontsize=8, frameon=False, loc='upper right')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No income-level CHN data found for 2021.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Affordability
# MAGIC *Income thresholds, affordable rent levels, and rental subsidy indicators (2021).*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Affordability: All characteristics
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'affordability'
# MAGIC ORDER BY geography, characteristic

# COMMAND ----------

# Chart 3.1: Median Household Income by Community (2021)
df = spark.sql("""
  SELECT geography, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'affordability'
    AND characteristic = 'Median household income'
    AND timeframe = '2021'
  ORDER BY indicator_value DESC
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(df['geography'], df['indicator_value'], color=SLATE, height=0.6)
    style_ax(ax, 'Median Household Income by Community (2021)')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    ax.invert_yaxis()
    for bar in bars:
        w = bar.get_width()
        ax.text(w + w * 0.01, bar.get_y() + bar.get_height()/2, f'${w:,.0f}',
                va='center', fontsize=9, color='#4A5568')
    plt.tight_layout()
    plt.show()
else:
    print("No median income data found for affordability section.")

# COMMAND ----------

# Chart 3.2: Affordable Rent Thresholds by Community (2021)
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'affordability'
    AND characteristic LIKE 'Rent at%AMHI'
    AND timeframe = '2021'
  ORDER BY geography, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(pivot))
    n_cols = len(pivot.columns)
    w = 0.8 / max(n_cols, 1)
    for i, col in enumerate(sorted(pivot.columns)):
        short_label = col.replace('Rent at ', '').replace(' of AMHI', '').replace(' AMHI', '')
        ax.bar(x + i * w - (n_cols - 1) * w / 2, pivot[col], w,
               label=short_label, color=PALETTE[i % len(PALETTE)])
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Affordable Monthly Rent Thresholds by Community (2021)', ylabel='Monthly Rent ($)')
    ax.legend(fontsize=8, frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No rent threshold data found.")

# COMMAND ----------

# Chart 3.3: Affordability Summary Table
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'affordability'
    AND timeframe = '2021'
  ORDER BY characteristic, geography
""").toPandas()

if len(df) > 0:
    pivot = df.pivot(index='characteristic', columns='geography', values='indicator_value')
    display(pivot)
else:
    print("No affordability data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Housing Supply
# MAGIC *Rental inventory, vacancy rates, primary vs secondary rental, and affordable unit changes.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Housing Supply: All characteristics and timeframes
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value,
# MAGIC   data_source
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'housing_supply'
# MAGIC ORDER BY geography, characteristic, timeframe

# COMMAND ----------

# Chart 4.1: Primary vs Secondary Rental Units
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'housing_supply'
    AND characteristic IN ('Primary Rental Units', 'Secondary Rental Units')
  ORDER BY geography
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(pivot))
    w = 0.35
    if 'Primary Rental Units' in pivot.columns:
        ax.bar(x - w/2, pivot['Primary Rental Units'], w, label='Primary', color=SLATE)
    if 'Secondary Rental Units' in pivot.columns:
        ax.bar(x + w/2, pivot['Secondary Rental Units'], w, label='Secondary', color=TEAL)
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Primary vs Secondary Rental Units', ylabel='Units')
    ax.legend(frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No primary/secondary rental data found.")

# COMMAND ----------

# Chart 4.2: Vacancy Rate by Community
df = spark.sql("""
  SELECT geography, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'housing_supply'
    AND characteristic = 'Vacancy Rate'
  ORDER BY timeframe, geography
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    # Use the latest timeframe available
    latest = df['timeframe'].max()
    latest_df = df[df['timeframe'] == latest].sort_values('indicator_value', ascending=True)

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(latest_df['geography'], latest_df['indicator_value'], color=TEAL, height=0.6)
    style_ax(ax, f'Vacancy Rate by Community ({latest})')
    ax.set_xlabel('Vacancy Rate (%)')
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 0.1, bar.get_y() + bar.get_height()/2, f'{w:.1f}%',
                va='center', fontsize=9, color='#4A5568')
    plt.tight_layout()
    plt.show()
else:
    print("No vacancy rate data found.")

# COMMAND ----------

# Chart 4.3: Tenure Split - Owner vs Renter Households
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'housing_supply'
    AND characteristic IN ('All Rental HHs', 'Private rental market housing units',
                           'Subsidized rental housing units')
  ORDER BY geography
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(pivot))
    colors_map = {
        'All Rental HHs': SLATE,
        'Private rental market housing units': TEAL,
        'Subsidized rental housing units': AQUA
    }
    for col in pivot.columns:
        c = colors_map.get(col, OCEAN)
        short = col.replace(' housing units', '').replace(' rental market', '')
        ax.barh(pivot.index, pivot[col], left=bottom, label=short, color=c, height=0.6)
        bottom += pivot[col].values
    style_ax(ax, 'Rental Housing Inventory by Type')
    ax.legend(fontsize=8, frameon=False, loc='lower right')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No rental inventory data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Growth Projections
# MAGIC *Projected household growth to 2031 by household size and income category.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Growth Projections: All characteristics
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'growth_projections'
# MAGIC ORDER BY geography, characteristic

# COMMAND ----------

# Chart 5.1: Projected Households by Income Category (2031)
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'growth_projections'
    AND characteristic LIKE '%AMHI%'
    AND timeframe = '2031'
  ORDER BY geography, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(pivot))
    n_cols = len(pivot.columns)
    w = 0.8 / max(n_cols, 1)
    for i, col in enumerate(sorted(pivot.columns)):
        short = col.replace(' of AMHI', '').replace(' AMHI', '')
        ax.bar(x + i * w - (n_cols - 1) * w / 2, pivot[col], w,
               label=short, color=PALETTE[i % len(PALETTE)])
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Projected Households by Income Category (2031)', ylabel='Households')
    ax.legend(fontsize=7, frameon=False, loc='upper right')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No income projection data found for 2031.")

# COMMAND ----------

# Chart 5.2: Projected Households by Size (2031)
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'growth_projections'
    AND characteristic LIKE '%person%'
    AND timeframe = '2031'
  ORDER BY geography, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    pivot = df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    bottom = np.zeros(len(pivot))
    for i, col in enumerate(sorted(pivot.columns)):
        ax.bar(pivot.index, pivot[col], bottom=bottom, label=col,
               color=PALETTE[i % len(PALETTE)])
        bottom += pivot[col].values
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Projected Households by Size (2031)', ylabel='Households')
    ax.legend(fontsize=8, frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No household size projection data found for 2031.")

# COMMAND ----------

# Chart 5.3: Growth Projections Summary Table
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'growth_projections'
    AND timeframe = '2031'
  ORDER BY characteristic, geography
""").toPandas()

if len(df) > 0:
    pivot = df.pivot(index='characteristic', columns='geography', values='indicator_value')
    display(pivot)
else:
    print("No growth projection data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Economic Indicators
# MAGIC *Building permits (number and value) by sector, EI recipients. 1992-2024.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Economic Indicators: All characteristics and timeframes
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value,
# MAGIC   data_source
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'economic_indicators'
# MAGIC ORDER BY geography, characteristic, timeframe

# COMMAND ----------

# Chart 6.1: Residential Building Permits Trend
df = spark.sql("""
  SELECT geography, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'economic_indicators'
    AND characteristic LIKE '%Residential%number%'
  ORDER BY geography, timeframe
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    df['timeframe'] = df['timeframe'].astype(str)

    fig, ax = plt.subplots(figsize=(14, 6))
    for i, geo in enumerate(df['geography'].unique()):
        geo_df = df[df['geography'] == geo].sort_values('timeframe')
        ax.plot(geo_df['timeframe'], geo_df['indicator_value'],
                label=geo, color=PALETTE[i % len(PALETTE)], linewidth=2, marker='o', markersize=4)
    style_ax(ax, 'Residential Building Permits Over Time', ylabel='Number of Permits')
    ax.legend(fontsize=8, frameon=False, loc='upper left')
    # Show every 5th x-label to avoid crowding
    ticks = ax.get_xticks()
    labels = [l.get_text() for l in ax.get_xticklabels()]
    if len(labels) > 10:
        for j, label in enumerate(ax.get_xticklabels()):
            if j % 5 != 0:
                label.set_visible(False)
    ax.tick_params(axis='x', rotation=45)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No residential building permit data found.")

# COMMAND ----------

# Chart 6.2: EI Recipients Trend
df = spark.sql("""
  SELECT geography, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'economic_indicators'
    AND characteristic = 'EI Recipients'
  ORDER BY geography, timeframe
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    df['timeframe'] = df['timeframe'].astype(str)

    fig, ax = plt.subplots(figsize=(14, 6))
    for i, geo in enumerate(df['geography'].unique()):
        geo_df = df[df['geography'] == geo].sort_values('timeframe')
        ax.plot(geo_df['timeframe'], geo_df['indicator_value'],
                label=geo, color=PALETTE[i % len(PALETTE)], linewidth=2, marker='o', markersize=4)
    style_ax(ax, 'EI Recipients Over Time', ylabel='Recipients')
    ax.legend(fontsize=8, frameon=False)
    ax.tick_params(axis='x', rotation=45)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No EI recipients data found.")

# COMMAND ----------

# Chart 6.3: Building Permits by Sector (latest year, all communities)
df = spark.sql("""
  SELECT geography, characteristic, indicator_value, timeframe
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'economic_indicators'
    AND characteristic LIKE 'Building Permits%number%'
    AND characteristic NOT LIKE '%value%'
  ORDER BY geography, timeframe DESC, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    # Get latest year per community
    latest_df = df.sort_values('timeframe', ascending=False).drop_duplicates(
        subset=['geography', 'characteristic'], keep='first')

    pivot = latest_df.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(pivot))
    n_cols = len(pivot.columns)
    w = 0.8 / max(n_cols, 1)
    for i, col in enumerate(pivot.columns):
        sector = col.replace('Building Permits - ', '').replace(' (number)', '')
        ax.bar(x + i * w - (n_cols - 1) * w / 2, pivot[col], w,
               label=sector, color=PALETTE[i % len(PALETTE)])
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Building Permits by Sector (Latest Year)', ylabel='Number of Permits')
    ax.legend(fontsize=8, frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No building permit sector data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Labour Force by Industry (Census 2021)
# MAGIC *Top NAICS sectors by employment count. Source: public_data.census.census_csd_2021*

# COMMAND ----------

# Chart 6.4: Labour Force by Industry (Top 8 NAICS sectors)
naics_industries = [
    '44-45 Retail trade',
    '23 Construction',
    '62 Health care and social assistance',
    '54 Professional, scientific and technical services',
    '48-49 Transportation and warehousing',
    '21 Mining, quarrying, and oil and gas extraction',
    '61 Educational services',
    '91 Public administration',
    '72 Accommodation and food services',
    '31-33 Manufacturing'
]
naics_str = ",".join([f"'{n}'" for n in naics_industries])

df_industry = spark.sql(f"""
  SELECT
    CASE
      WHEN g.geography LIKE '%Rocky View%' THEN 'Rocky View'
      ELSE SPLIT(g.geography, ',')[0]
    END AS community,
    c.characteristic AS industry,
    c.indicator_value AS workers
  FROM public_data.census.census_csd_2021 c
  JOIN public_data.housing.hna_geography_dim g ON c.geography_id = g.geography_id
  WHERE c.characteristic IN ({naics_str})
  ORDER BY community, workers DESC
""").toPandas()

if not df_industry.empty:
    # Aggregate top 6 industries by total workers across all communities
    top6 = df_industry.groupby('industry')['workers'].sum().nlargest(6).index.tolist()
    df_top = df_industry[df_industry['industry'].isin(top6)]

    # Shorten labels
    short = {
        '44-45 Retail trade': 'Retail',
        '23 Construction': 'Construction',
        '62 Health care and social assistance': 'Healthcare',
        '54 Professional, scientific and technical services': 'Professional Svcs',
        '48-49 Transportation and warehousing': 'Transportation',
        '21 Mining, quarrying, and oil and gas extraction': 'Oil & Gas/Mining',
        '61 Educational services': 'Education',
        '91 Public administration': 'Public Admin',
        '72 Accommodation and food services': 'Food & Accomm',
        '31-33 Manufacturing': 'Manufacturing'
    }
    df_top['short'] = df_top['industry'].map(short).fillna(df_top['industry'])

    pivot = df_top.pivot_table(index='short', columns='community', values='workers', fill_value=0)

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = [SLATE, TEAL, AQUA, NAVY, '#2C5282', '#0E8C86', '#7EDDD5']
    pivot.plot(kind='barh', ax=ax, color=colors[:len(pivot.columns)])
    style_ax(ax, 'Labour Force by Industry - Top 6 NAICS Sectors (Census 2021)', ylabel='')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    ax.legend(fontsize=8, frameon=False, loc='lower right')
    plt.tight_layout()
    plt.show()
else:
    print("No Census industry data found. Check public_data.census.census_csd_2021 geography_id join.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Priority Populations
# MAGIC *CHN rates for 18 priority groups including youth, seniors, Indigenous, newcomers, and persons with disabilities (2021).*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Priority Populations: All characteristics
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'priority_populations'
# MAGIC ORDER BY geography, characteristic

# COMMAND ----------

# Chart 7.1: Priority Group CHN Rate - Horizontal Bar (sorted)
# Average across all communities
df = spark.sql("""
  SELECT characteristic, AVG(indicator_value) AS avg_rate, COUNT(*) AS n_communities
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'priority_populations'
    AND characteristic LIKE '%rate%'
    AND timeframe = '2021'
  GROUP BY characteristic
  ORDER BY avg_rate DESC
""").toPandas()

if len(df) > 0:
    df['avg_rate'] = df['avg_rate'].astype(float)
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = [TEAL if v > df['avg_rate'].median() else SLATE for v in df['avg_rate']]
    bars = ax.barh(df['characteristic'], df['avg_rate'], color=colors, height=0.7)
    style_ax(ax, 'Priority Population CHN Rates (Avg Across Communities, 2021)')
    ax.set_xlabel('Average CHN Rate (%)')
    ax.invert_yaxis()
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 0.3, bar.get_y() + bar.get_height()/2, f'{w:.1f}%',
                va='center', fontsize=8, color='#4A5568')
    plt.tight_layout()
    plt.show()
else:
    print("No priority population rate data found.")

# COMMAND ----------

# Chart 7.2: Priority Populations Comparison Across Communities
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'priority_populations'
    AND characteristic LIKE '%rate%'
    AND timeframe = '2021'
  ORDER BY geography, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    # Pick top 6 priority groups by average rate for readability
    top_groups = df.groupby('characteristic')['indicator_value'].mean().nlargest(6).index.tolist()
    filtered = df[df['characteristic'].isin(top_groups)]
    pivot = filtered.pivot(index='geography', columns='characteristic', values='indicator_value').fillna(0)

    fig, ax = plt.subplots(figsize=(14, 6))
    x = np.arange(len(pivot))
    n_cols = len(pivot.columns)
    w = 0.8 / max(n_cols, 1)
    for i, col in enumerate(pivot.columns):
        short = col.replace(' CHN rate', '').replace(' (rate)', '')
        ax.bar(x + i * w - (n_cols - 1) * w / 2, pivot[col], w,
               label=short, color=PALETTE[i % len(PALETTE)])
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Top Priority Population CHN Rates by Community (2021)', ylabel='CHN Rate (%)')
    ax.legend(fontsize=7, frameon=False, loc='upper right')
    plt.tight_layout()
    plt.show()
else:
    print("No priority population comparison data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Rental Market
# MAGIC *Average rents, vacancy rates by bedroom type, housing starts/completions, and rent trends.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rental Market: All characteristics, timeframes, and data sources
# MAGIC SELECT
# MAGIC   geography,
# MAGIC   characteristic,
# MAGIC   timeframe,
# MAGIC   indicator_value,
# MAGIC   data_source
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC WHERE dashboard_section = 'rental_market'
# MAGIC ORDER BY geography, characteristic, timeframe

# COMMAND ----------

# Chart 8.1: Vacancy Rate by Community (latest year)
df = spark.sql("""
  SELECT geography, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'rental_market'
    AND characteristic LIKE 'Vacancy Rate%Total%'
  ORDER BY geography, timeframe DESC
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    latest = df.sort_values('timeframe', ascending=False).drop_duplicates(subset='geography', keep='first')
    latest = latest.sort_values('indicator_value', ascending=True)
    yr = latest['timeframe'].iloc[0]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(latest['geography'], latest['indicator_value'], color=TEAL, height=0.6)
    style_ax(ax, f'Vacancy Rate by Community ({yr})')
    ax.set_xlabel('Vacancy Rate (%)')
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 0.1, bar.get_y() + bar.get_height()/2, f'{w:.1f}%',
                va='center', fontsize=9, color='#4A5568')
    plt.tight_layout()
    plt.show()
else:
    # Try without 'Total' qualifier
    df2 = spark.sql("""
      SELECT geography, timeframe, indicator_value
      FROM public_data.housing.hna_dashboard_2006_2031
      WHERE dashboard_section = 'rental_market'
        AND characteristic LIKE 'Vacancy Rate%'
      ORDER BY geography, timeframe DESC
    """).toPandas()
    if len(df2) > 0:
        df2['indicator_value'] = df2['indicator_value'].astype(float)
        latest = df2.sort_values('timeframe', ascending=False).drop_duplicates(subset='geography', keep='first')
        latest = latest.sort_values('indicator_value', ascending=True)
        yr = latest['timeframe'].iloc[0]
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(latest['geography'], latest['indicator_value'], color=TEAL, height=0.6)
        style_ax(ax, f'Vacancy Rate by Community ({yr})')
        ax.set_xlabel('Vacancy Rate (%)')
        plt.tight_layout()
        plt.show()
    else:
        print("No vacancy rate data found in rental_market section.")

# COMMAND ----------

# Chart 8.2: Average Rent by Bedroom Type
df = spark.sql("""
  SELECT geography, characteristic, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'rental_market'
    AND characteristic LIKE 'Average Rent%'
  ORDER BY geography, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    # Use latest data per geography/characteristic
    pivot = df.pivot_table(index='geography', columns='characteristic',
                           values='indicator_value', aggfunc='last').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(pivot))
    n_cols = len(pivot.columns)
    w = 0.8 / max(n_cols, 1)
    for i, col in enumerate(sorted(pivot.columns)):
        short = col.replace('Average Rent - ', '').replace('Average Rent ', '')
        ax.bar(x + i * w - (n_cols - 1) * w / 2, pivot[col], w,
               label=short, color=PALETTE[i % len(PALETTE)])
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, 'Average Rent by Bedroom Type', ylabel='Monthly Rent ($)')
    ax.legend(fontsize=8, frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No average rent data found.")

# COMMAND ----------

# Chart 8.3: Rent Trend Over Time
df = spark.sql("""
  SELECT geography, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'rental_market'
    AND (characteristic LIKE 'Avg Monthly Rent%' OR characteristic LIKE 'Average Rent%Total%')
  ORDER BY geography, timeframe
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    df['timeframe'] = df['timeframe'].astype(str)

    fig, ax = plt.subplots(figsize=(14, 6))
    for i, geo in enumerate(df['geography'].unique()):
        geo_df = df[df['geography'] == geo].sort_values('timeframe')
        ax.plot(geo_df['timeframe'], geo_df['indicator_value'],
                label=geo, color=PALETTE[i % len(PALETTE)], linewidth=2, marker='o', markersize=4)
    style_ax(ax, 'Average Rent Trend Over Time', ylabel='Monthly Rent ($)')
    ax.legend(fontsize=8, frameon=False)
    ax.tick_params(axis='x', rotation=45)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No rent trend data found.")

# COMMAND ----------

# Chart 8.4: Vacancy Rate Trend Over Time
df = spark.sql("""
  SELECT geography, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'rental_market'
    AND characteristic LIKE 'Vacancy Rate%'
  ORDER BY geography, timeframe
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    df['timeframe'] = df['timeframe'].astype(str)
    # Aggregate if multiple bedroom types - take the one with most data
    counts = df.groupby('geography').size()

    fig, ax = plt.subplots(figsize=(14, 6))
    for i, geo in enumerate(df['geography'].unique()):
        geo_df = df[df['geography'] == geo].sort_values('timeframe')
        # Average across bedroom types if multiple
        geo_agg = geo_df.groupby('timeframe')['indicator_value'].mean().reset_index()
        ax.plot(geo_agg['timeframe'], geo_agg['indicator_value'],
                label=geo, color=PALETTE[i % len(PALETTE)], linewidth=2, marker='o', markersize=4)
    style_ax(ax, 'Vacancy Rate Trend Over Time', ylabel='Vacancy Rate (%)')
    ax.legend(fontsize=8, frameon=False)
    ax.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    plt.show()
else:
    print("No vacancy rate trend data found.")

# COMMAND ----------

# Chart 8.5: Housing Starts by Structure Type
df = spark.sql("""
  SELECT geography, characteristic, timeframe, indicator_value
  FROM public_data.housing.hna_dashboard_2006_2031
  WHERE dashboard_section = 'rental_market'
    AND characteristic LIKE 'Housing Starts%'
  ORDER BY geography, timeframe, characteristic
""").toPandas()

if len(df) > 0:
    df['indicator_value'] = df['indicator_value'].astype(float)
    # Latest year, stacked by structure type
    latest_yr = df['timeframe'].max()
    latest = df[df['timeframe'] == latest_yr]
    pivot = latest.pivot_table(index='geography', columns='characteristic',
                                values='indicator_value', aggfunc='sum').fillna(0)

    fig, ax = plt.subplots(figsize=(12, 6))
    bottom = np.zeros(len(pivot))
    for i, col in enumerate(pivot.columns):
        short = col.replace('Housing Starts - ', '').replace('Housing Starts ', '')
        ax.bar(pivot.index, pivot[col], bottom=bottom, label=short,
               color=PALETTE[i % len(PALETTE)])
        bottom += pivot[col].values
    ax.set_xticklabels(pivot.index, rotation=45, ha='right')
    style_ax(ax, f'Housing Starts by Structure Type ({latest_yr})', ylabel='Units')
    ax.legend(fontsize=8, frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    plt.tight_layout()
    plt.show()
else:
    print("No housing starts data found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Data Completeness Summary

# COMMAND ----------

# Data Completeness Summary
summary = spark.sql("""
  SELECT
    dashboard_section,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT geography) AS communities,
    COUNT(DISTINCT characteristic) AS characteristics,
    COUNT(DISTINCT timeframe) AS timeframes,
    MIN(timeframe) AS earliest_year,
    MAX(timeframe) AS latest_year,
    SUM(CASE WHEN indicator_value IS NULL THEN 1 ELSE 0 END) AS null_values,
    ROUND(100.0 * SUM(CASE WHEN indicator_value IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_complete
  FROM public_data.housing.hna_dashboard_2006_2031
  GROUP BY dashboard_section
  ORDER BY dashboard_section
""").toPandas()

print("=" * 90)
print("DATA COMPLETENESS SUMMARY")
print("=" * 90)
print(f"{'Section':<25} {'Rows':>6} {'Comm':>5} {'Chars':>6} {'TFs':>4} {'Range':<12} {'Nulls':>6} {'Complete':>9}")
print("-" * 90)
for _, row in summary.iterrows():
    print(f"{row['dashboard_section']:<25} {row['total_rows']:>6} {row['communities']:>5} "
          f"{row['characteristics']:>6} {row['timeframes']:>4} "
          f"{row['earliest_year']}-{row['latest_year']:<6} "
          f"{row['null_values']:>6} {row['pct_complete']:>8.1f}%")
print("-" * 90)
print(f"{'TOTAL':<25} {summary['total_rows'].sum():>6}")
print("=" * 90)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detailed characteristic list per section (for reference)
# MAGIC SELECT
# MAGIC   dashboard_section,
# MAGIC   characteristic,
# MAGIC   COUNT(*) AS rows,
# MAGIC   COUNT(DISTINCT geography) AS communities,
# MAGIC   COUNT(DISTINCT timeframe) AS timeframes
# MAGIC FROM public_data.housing.hna_dashboard_2006_2031
# MAGIC GROUP BY dashboard_section, characteristic
# MAGIC ORDER BY dashboard_section, characteristic

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Notes
# MAGIC - All data sourced from `public_data.housing.hna_dashboard_2006_2031`
# MAGIC - Charts use HelpSeeker brand colors: Slate Blue, Teal, Aqua, Deep Navy
# MAGIC - Where characteristic names in LIKE clauses do not match actual data, the chart cell prints a "not found" message -- check the SQL exploration cells above to identify the exact characteristic strings
# MAGIC - This notebook is designed to run on warehouse `a7e9ada5cd37e1c7` (Pro SQL Starter)
