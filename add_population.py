# This file merges the existing vaccination_cleaned file with population data from the owid-covid-data file

mport pandas as pd

# Load cleaned vaccination data
vacc_df = pd.read_csv('vaccinations_cleaned.csv')

# Load OWID COVID data (which includes population)
covid_df = pd.read_csv('owid-covid-data.csv')

# Extract one population value per country
# (population is constant, so we just take the last known value per location)
population_df = covid_df[['location', 'population']].dropna().drop_duplicates(subset='location', keep='last')

# Merge population into vaccination data
merged_df = pd.merge(vacc_df, population_df, on='location', how='left')

# Calculate unvaccinated metrics
merged_df['unvaccinated'] = merged_df['population'] - merged_df['people_vaccinated']
merged_df['unvaccinated_per_hundred'] = 100 - merged_df['people_vaccinated_per_hundred']

# Merge with vaccinations_with_population
merged_df.to_csv('vaccinations_cleaned_with_population.csv', index=False)

# Save to a csv file
print("Merged dataset saved as 'vaccinations_cleaned_with_population.csv'")
