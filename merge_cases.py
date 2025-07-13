import pandas as pd

# Load the wide-format files
new_cases = pd.read_csv('new_cases.csv')
total_cases = pd.read_csv('total_cases.csv')

# Melt both datasets into long format
new_cases_long = new_cases.melt(id_vars='date', var_name='location', value_name='new_cases')
total_cases_long = total_cases.melt(id_vars='date', var_name='location', value_name='total_cases')

# Convert 'date' columns to datetime
new_cases_long['date'] = pd.to_datetime(new_cases_long['date'])
total_cases_long['date'] = pd.to_datetime(total_cases_long['date'])

# Merge them on location and date
merged_cases = pd.merge(new_cases_long, total_cases_long, on=['location', 'date'], how='outer')

# Save to a new csv file
merged_cases.to_csv('cases_combined.csv', index=False)
print("Merged file saved as 'cases_combined.csv'")
