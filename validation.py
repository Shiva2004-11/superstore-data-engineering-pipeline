import pandas as pd
import sys

def validate_data(df):
    if df.empty:
        return False
    has_row_id = 'row_id' in df.columns
    return df.isnull().sum().sum() == 0 and df.duplicated().sum() == 0 and has_row_id

if __name__ == "__main__":
    try:
        df = pd.read_csv("staging_superstore.csv")
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()

    if df.empty:
        print("No data to validate. Skipping validation.")
        sys.exit(0)

    print("=" * 60)
    print("Validation Report")
    print("=" * 60)

    print(f"Rows : {len(df)}")
    print(f"Columns : {len(df.columns)}")

    print("\nMissing Values")
    print(df.isnull().sum())

    print("\nDuplicate Rows :", df.duplicated().sum())

    print("\nChecking for 'row_id' column")
    has_row_id = 'row_id' in df.columns
    print(f"row_id present: {has_row_id}")

    print("=" * 60)

    if validate_data(df):
        print("Validation Successful")
    else:
        print("Validation Failed")
        print("=" * 60)
        sys.exit(1)

    print("=" * 60)