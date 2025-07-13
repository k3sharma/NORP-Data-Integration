# Creates final cleaned dataset 

import pandas as pd

# Load the vaccination (with population) and case datasets
vacc_df = pd.read_csv('vaccinations_cleaned_with_population.csv')
cases_df = pd.read_csv('cases_combined.csv')

# Convert 'date' columns to datetime format
vacc_df['date'] = pd.to_datetime(vacc_df['date'])
cases_df['date'] = pd.to_datetime(cases_df['date'])

# Merge on 'location' and 'date'
merged = pd.merge(vacc_df, cases_df, on=['location', 'date'], how='inner')

# Save the merged file
merged.to_csv('vaccinations_cases_merged.csv', index=False)

df = pd.read_csv('vaccinations_cases_merged.csv')

# Classify each row based on it's vaccination coverage (how much of a country's population has been vaccinated at a given time)
def categorize_vax(val):
    if val >= 70:
        return 'High (>=70%)'
    elif val >= 40:
        return 'Medium (40–69%)'
    else:
        return 'Low (<40%)'

df['vaccination_level'] = df['people_vaccinated_per_hundred'].apply(categorize_vax)

# Future new cases
df['future_new_cases'] = df.groupby('location')['new_cases'].shift(-14)

# Per capita infection rate (per 100,000 people)
df['new_cases_per_100k'] = (df['new_cases'] / df['population']) * 100_000
df['future_cases_per_100k'] = (df['future_new_cases'] / df['population']) * 100_000


df.to_csv('vaccinations_cases_merged.csv', index=False)

print("Merged dataset saved as 'vaccinations_cases_merged.csv'")
