# # """
# # process_owid_covid.py
# # =====================
# # A one‑stop script that replicates the end‑to‑end pipeline of:
# #   1. clean_vaccinations.py
# #   2. add_population.py
# #   3. merge_cases.py
# #   4. vaccinations_and_cases.py
# # using **only** the raw `owid-covid-data.csv` file as input.

# # It produces `vaccinations_cases_merged.csv` with identical columns:
# # location,date,people_vaccinated,people_fully_vaccinated,total_vaccinations,
# # daily_vaccinations,people_vaccinated_per_hundred,people_fully_vaccinated_per_hundred,
# # total_vaccinations_per_hundred,population,unvaccinated,unvaccinated_per_hundred,
# # new_cases,total_cases,vaccination_level,future_new_cases,new_cases_per_100k,future_cases_per_100k
# # """

# import pandas as pd
# from pathlib import Path

# RAW_FILE = Path('owid-covid-data.csv')
# OUTPUT_FILE = Path('final.csv')

# # Load OWID master dataset

# print('Loading raw OWID data …')
# df = pd.read_csv(RAW_FILE,
#                  usecols=[
#                      'location', 'date',
#                      # vaccination metrics
#                      'people_vaccinated', 'people_fully_vaccinated', 'total_vaccinations',
#                      'daily_vaccinations', 'people_vaccinated_per_hundred',
#                      'people_fully_vaccinated_per_hundred', 'total_vaccinations_per_hundred',
#                      # population
#                      'population',
#                      # case metrics
#                      'new_cases', 'total_cases',
#                  ])

# # Keep only rows that are real countries/territories (filter out aggregates like "World")
# aggr_entities = {
#     'World', 'International', 'Europe', 'European Union', 'Upper middle income',
#     'Lower middle income', 'Low income', 'High income',
# }
# df = df[~df['location'].isin(aggr_entities)].copy()

# # Basic cleaning
# # Ensure proper dtypes
# numeric_cols = [c for c in df.columns if c not in {'location', 'date'}]
# df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

# df['date'] = pd.to_datetime(df['date'])

# # Fill forward country‑level population once (population is constant per location)
# df['population'] = df.groupby('location')['population'].transform('last')

# # Compute additional vaccination + population metrics

# print('Deriving vaccination, population and case features …')

# df['unvaccinated'] = df['population'] - df['people_vaccinated']
# df['unvaccinated_per_hundred'] = 100 - df['people_vaccinated_per_hundred']

# # Vaccination coverage category

# def categorize_vax(pct: float) -> str:
#     if pd.isna(pct):
#         return 'Unknown'
#     if pct >= 70:
#         return 'High (>=70%)'
#     if pct >= 40:
#         return 'Medium (40–69%)'
#     return 'Low (<40%)'

# # Vectorised apply for speed
# vax_pct = df['people_vaccinated_per_hundred']
# df['vaccination_level'] = pd.cut(
#     vax_pct,
#     bins=[-float('inf'), 40, 70, float('inf')],
#     labels=['Low (<40%)', 'Medium (40–69%)', 'High (>=70%)']
# ).astype(str)
# # Cut leaves NaN for <40, so patch manually
# low_mask = vax_pct < 40
# na_mask = vax_pct.isna()
# df.loc[low_mask, 'vaccination_level'] = 'Low (<40%)'
# df.loc[na_mask, 'vaccination_level'] = 'Unknown'

# # Compute forward‑looking case metrics (14‑day horizon)

# df.sort_values(['location', 'date'], inplace=True)
# df['future_new_cases'] = df.groupby('location')['new_cases'].shift(-14)

# # Per‑capita rates
# PER_CAPITA_FACTOR = 100_000

# df['new_cases_per_100k'] = (df['new_cases'] / df['population']) * PER_CAPITA_FACTOR
# df['future_cases_per_100k'] = (df['future_new_cases'] / df['population']) * PER_CAPITA_FACTOR

# # Final column ordering & export

# cols_order = [
#     'location', 'date',
#     'people_vaccinated', 'people_fully_vaccinated', 'total_vaccinations',
#     'daily_vaccinations', 'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
#     'total_vaccinations_per_hundred', 'population', 'unvaccinated',
#     'unvaccinated_per_hundred', 'new_cases', 'total_cases', 'vaccination_level',
#     'future_new_cases', 'new_cases_per_100k', 'future_cases_per_100k'
# ]

# missing_cols = [c for c in cols_order if c not in df.columns]
# if missing_cols:
#     raise ValueError(f'Missing expected columns from OWID data: {missing_cols}')

# df[cols_order].to_csv(OUTPUT_FILE, index=False)
# print(f'Saved merged dataset to {OUTPUT_FILE.resolve()}')





# import pandas as pd

# # Load OWID COVID-19 data
# df = pd.read_csv('owid-covid-data.csv', parse_dates=['date'])

# # Drop rows missing location/date/population
# df = df.dropna(subset=['location', 'date', 'population'])

# # === STEP 1: Clean and select relevant columns ===
# keep_cols = [
#     'location', 'date', 'population',
#     'people_vaccinated', 'people_fully_vaccinated', 'total_vaccinations',
#     'daily_vaccinations', 'people_vaccinated_per_hundred',
#     'people_fully_vaccinated_per_hundred', 'total_vaccinations_per_hundred',
#     'new_cases', 'total_cases'
# ]
# df = df[[col for col in keep_cols if col in df.columns]]

# # === STEP 2: Forward-fill key vaccination metrics ===
# df = df.sort_values(['location', 'date'])
# ffill_cols = ['people_vaccinated', 'people_fully_vaccinated', 'total_vaccinations']
# df[ffill_cols] = df.groupby('location')[ffill_cols].ffill()
# df.fillna(0, inplace=True)

# # === STEP 3: Compute unvaccinated people ===
# df['unvaccinated'] = df['population'] - df['people_vaccinated']
# df['unvaccinated_per_hundred'] = 100 - df['people_vaccinated_per_hundred']

# # === STEP 4: Classify vaccination coverage levels ===
# def categorize_vax(val):
#     if val >= 70:
#         return 'High (>=70%)'
#     elif val >= 40:
#         return 'Medium (40–69%)'
#     else:
#         return 'Low (<40%)'
# df['vaccination_level'] = df['people_vaccinated_per_hundred'].apply(categorize_vax)

# # === STEP 5: Calculate future cases (14 days ahead) ===
# df['future_new_cases'] = df.groupby('location')['new_cases'].shift(-14)

# # === STEP 6: Normalize case rates per 100k people ===
# df['new_cases_per_100k'] = (df['new_cases'] / df['population']) * 100_000
# df['future_cases_per_100k'] = (df['future_new_cases'] / df['population']) * 100_000

# # === STEP 7: Save final file ===
# df.to_csv('final.csv', index=False)
# print("✅ Saved final file as 'final.csv'")


import pandas as pd

# Load vaccination data
vacc_df = pd.read_csv('vaccinations.csv', parse_dates=['date'])

# Keep these columns only
columns_to_keep = [
    'location', 'date', 'people_vaccinated', 'people_fully_vaccinated',
    'total_vaccinations', 'daily_vaccinations',
    'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
    'total_vaccinations_per_hundred'
]

# Keep only existing columns
vacc_df = vacc_df[[col for col in columns_to_keep if col in vacc_df.columns]]

# Forward-fill key values
vacc_df = vacc_df.sort_values(['location', 'date'])
fill_cols = ['people_vaccinated', 'people_fully_vaccinated', 'total_vaccinations']
vacc_df[fill_cols] = vacc_df.groupby('location')[fill_cols].ffill()

# Replace NaNs with 0s
vacc_df.fillna(0, inplace=True)

# Load raw OWID COVID full dataset
covid_df = pd.read_csv('owid-covid-data.csv', parse_dates=['date'])

# Extract the population value per country
population_df = covid_df[['location', 'population']].dropna().drop_duplicates('location', keep='last')

# Extract relevant case data
case_df = covid_df[['location', 'date', 'new_cases', 'total_cases']]

# Merge vaccination data with population
merged_df = pd.merge(vacc_df, population_df, on='location', how='left')

# Merge with case data by location and date
merged_df = pd.merge(merged_df, case_df, on=['location', 'date'], how='left')

# Calculate the unvaccinated metrics
merged_df['unvaccinated'] = merged_df['population'] - merged_df['people_vaccinated']
merged_df['unvaccinated_per_hundred'] = 100 - merged_df['people_vaccinated_per_hundred']

# Classify each row based on it's vaccination coverage (how much of a country's population has been vaccinated at a given time)
def categorize_vax(val):
    if val >= 70:
        return 'High (>=70%)'
    elif val >= 40:
        return 'Medium (40–69%)'
    else:
        return 'Low (<40%)'

merged_df['vaccination_level'] = merged_df['people_vaccinated_per_hundred'].apply(categorize_vax)

# Shift to get new cases 14 days in the future
merged_df['future_new_cases'] = merged_df.groupby('location')['new_cases'].shift(-14)

# Per capita infection rate (per 100,000 people)
merged_df['new_cases_per_100k'] = (merged_df['new_cases'] / merged_df['population']) * 100_000
merged_df['future_cases_per_100k'] = (merged_df['future_new_cases'] / merged_df['population']) * 100_000

# Final cleanup
merged_df.fillna(0, inplace=True)

# Save to CSV
merged_df.to_csv('final_cleaned.csv', index=False)
print("Final dataset saved as 'final_cleaned.csv'")
