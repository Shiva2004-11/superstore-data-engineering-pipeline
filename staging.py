import pandas as pd

def clean_data(df):
    # Remove duplicate records
    df = df.drop_duplicates()

    # Remove rows with missing values
    df = df.dropna()

    # Rename columns
    df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]

    # Parse date columns
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    if 'ship_date' in df.columns:
        df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')
        
    return df

if __name__ == "__main__":
    print("=" * 60)
    print("Loading Processed Data")
    print("=" * 60)

    try:
        df = pd.read_csv("processed_superstore.csv")
    except pd.errors.EmptyDataError:
        print("No records found in processed_superstore.csv")
        df = pd.DataFrame()

    if df.empty:
        print("Staging Completed Successfully (No Data)")
        # Create empty staging file so validation doesn't crash on FileNotFoundError
        with open("staging_superstore.csv", "w") as f:
            f.write("")
        exit(0)

    print(f"Records Loaded : {len(df)}")

    df = clean_data(df)

    print(f"Records after Cleaning : {len(df)}")

    # Save staging file
    df.to_csv("staging_superstore.csv", index=False)

    print("=" * 60)
    print("Staging Completed Successfully")
    print("Output File : staging_superstore.csv")
    print("=" * 60)