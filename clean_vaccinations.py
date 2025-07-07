# Creates a cleaned vaccination file

import pandas as pd

# Load the OWID vaccinations dataset
df = pd.read_csv('vaccinations.csv')

# Define columns to keep (adjusted to match actual file contents)
columns_to_keep = [
    'location', 'date', 'people_vaccinated',
    'people_fully_vaccinated', 'total_vaccinations',
    'daily_vaccinations', 'people_vaccinated_per_hundred',
    'people_fully_vaccinated_per_hundred', 'total_vaccinations_per_hundred'
]

# Keep only the columns that exist
existing_columns = [col for col in columns_to_keep if col in df.columns]
df = df[existing_columns]

# Convert date to datetime format
df['date'] = pd.to_datetime(df['date'])

# Sort and forward-fill missing values within each location
df = df.sort_values(['location', 'date'])
fill_cols = [col for col in ['people_vaccinated', 'people_fully_vaccinated', 'total_vaccinations'] if col in df.columns]
df[fill_cols] = df.groupby('location')[fill_cols].ffill()

# Replace missing numeric values by setting them to 0
df.fillna(0, inplace=True)

# Load OWID COVID full dataset
covid_df = pd.read_csv('owid-covid-data.csv')

# Get the latest population value per country
population_df = covid_df[['location', 'population']].dropna().drop_duplicates(subset='location', keep='last')

# Load your cleaned vaccinations dataset
vacc_df = pd.read_csv('vaccinations_cleaned.csv')

# Merge on 'location'
merged_df = pd.merge(vacc_df, population_df, on='location', how='left')

# Save the cleaned dataset
df.to_csv('vaccinations_cleaned.csv', index=False)
print("Cleaned dataset saved as 'vaccinations_cleaned.csv'")