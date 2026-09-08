import pandas as pd
import pytest
from staging import clean_data

def test_column_standardization():
    df = pd.DataFrame({
        "Row ID": [1],
        "Order Date": ["2023-01-01"],
        "Sub-Category": ["Phones"]
    })
    
    cleaned_df = clean_data(df)
    
    # Assert columns are renamed properly
    assert list(cleaned_df.columns) == ["row_id", "order_date", "sub_category"]

def test_duplicate_removal():
    df = pd.DataFrame({
        "row_id": [1, 1, 2],
        "category": ["A", "A", "B"]
    })
    
    cleaned_df = clean_data(df)
    
    assert len(cleaned_df) == 2
    assert list(cleaned_df["row_id"]) == [1, 2]

def test_missing_value_handling():
    df = pd.DataFrame({
        "row_id": [1, 2, 3],
        "category": ["A", None, "C"]
    })
    
    cleaned_df = clean_data(df)
    
    assert len(cleaned_df) == 2
    assert list(cleaned_df["row_id"]) == [1, 3]

def test_date_conversion():
    df = pd.DataFrame({
        "row_id": [1],
        "order_date": ["1/5/2023"],
        "ship_date": ["2023-01-10"]
    })
    
    cleaned_df = clean_data(df)
    
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df['order_date'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df['ship_date'])
    
def test_edge_case_empty_dataframe():
    df = pd.DataFrame()
    cleaned_df = clean_data(df)
    
    assert cleaned_df.empty
