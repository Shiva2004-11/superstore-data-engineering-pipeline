import pandas as pd
import os
import matplotlib.pyplot as plt

def run_eda():
    print("=" * 60)
    print("Starting Exploratory Data Analysis (EDA)")
    print("=" * 60)

    try:
        df = pd.read_csv("Sample - Superstore.csv", encoding="latin1")
    except Exception as e:
        print(f"Failed to load dataset: {e}")
        return

    # Create output directory
    output_dir = "eda_output"
    os.makedirs(output_dir, exist_ok=True)

    # Basic statistics
    print(f"Dataset Shape: {df.shape}")
    print("\nColumns and Data Types:")
    print(df.dtypes)
    print("\nMissing Values:")
    print(df.isnull().sum())
    print("\nDuplicate Rows:")
    print(df.duplicated().sum())
    
    print("\nUnique Customers:", df["Customer ID"].nunique())
    print("Unique Products:", df["Product ID"].nunique())
    
    print("\nSales Statistics:")
    print(df["Sales"].describe())
    
    print("\nProfit Statistics:")
    print(df["Profit"].describe())

    # Visualizations
    plt.figure(figsize=(10, 6))
    df.groupby("Category")["Sales"].sum().plot(kind="bar", color="skyblue")
    plt.title("Total Sales by Category")
    plt.ylabel("Sales")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "sales_by_category.png"))
    plt.close()

    plt.figure(figsize=(10, 6))
    df.groupby("Region")["Profit"].sum().plot(kind="bar", color="lightgreen")
    plt.title("Total Profit by Region")
    plt.ylabel("Profit")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "profit_by_region.png"))
    plt.close()

    print("=" * 60)
    print(f"EDA Completed Successfully. Plots saved to {output_dir}/")
    print("=" * 60)

if __name__ == "__main__":
    run_eda()
