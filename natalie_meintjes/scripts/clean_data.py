import pandas as pd

# Load raw data
df = pd.read_csv('data/owid_covid_data.csv')

# Exclude aggregate entities
excluded_entities = [
    'World', 'Africa', 'Asia', 'Europe', 'European Union',
    'International', 'Low income', 'High income',
    'North America', 'Oceania', 'South America'
]

def clean_covid_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean OWID COVID data by removing aggregates and invalid rows."""
df = df[~df['country'].isin(excluded_entities)]

# Drop rows with missing country codes (likely invalid rows)
df = df[df['code'].notna()]

if __name__ == "__main__":
    # Load raw data
    input_path = 'data/owid_covid_data.csv'
    output_path = 'data/cleaned_covid.csv'

# Save cleaned data
df_raw = pd.read_csv(input_path)
df_cleaned = clean_covid_data(df_raw)
df_cleaned.to_csv(output_path, index=False)

print(f"Cleaned dataset saved with {df.shape[0]:,} rows and {df.shape[1]} columns.")
