import pandas as pd

def clean_data(input_file, output_file):
    # Load Excel file
    df = pd.read_excel(input_file)
    
    # Check for missing values 
    print("Missing values per column:")
    print(df.isnull().sum())

    # rename 
    df.rename(columns={
    "discountPercent": "discount_percent",
    "availableQuantity": "available_quantity",
    "discountedSellingPrice": "discounted_selling_price",
    "weightInGms": "weight_in_gms",
    "outOfStock": "out_of_stock"
}, inplace=True)

    # Convert boolean to  int
    df['out_of_stock'] = df['out_of_stock'].astype(int)

    # Save cleaned data to CSV
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")

if __name__ == "__main__":
    input_file = "/Users/jaypatel/Downloads/zepto_v1.xlsx"
    output_file = "/Users/jaypatel/Downloads/zepto_data_cleaned.csv"
    clean_data(input_file, output_file)

