import pandas as pd

# Load raw data
df = pd.read_csv('data/owid_covid_data.csv')

# Exclude aggregate entities
excluded_entities = [
    'World', 'Africa', 'Asia', 'Europe', 'European Union',
    'International', 'Low income', 'High income',
    'North America', 'Oceania', 'South America'
]
df = df[~df['country'].isin(excluded_entities)]

# Drop rows with missing country codes (likely invalid rows)
df = df[df['code'].notna()]

# Save cleaned data
df.to_csv('data/cleaned_covid.csv', index=False)

print(f"Cleaned dataset saved with {df.shape[0]:,} rows and {df.shape[1]} columns.")
