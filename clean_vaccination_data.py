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
