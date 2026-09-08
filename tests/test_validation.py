import pandas as pd
import pytest
from validation import validate_data

def test_valid_data():
    df = pd.DataFrame({
        "row_id": [1, 2],
        "category": ["A", "B"]
    })
    
    assert validate_data(df)

def test_invalid_data_missing_values():
    df = pd.DataFrame({
        "row_id": [1, 2],
        "category": ["A", None]
    })
    
    assert not validate_data(df)

def test_invalid_data_duplicates():
    df = pd.DataFrame({
        "row_id": [1, 1],
        "category": ["A", "A"]
    })
    
    assert not validate_data(df)

def test_invalid_data_missing_row_id():
    df = pd.DataFrame({
        "id": [1, 2],
        "category": ["A", "B"]
    })
    
    assert not validate_data(df)

def test_edge_case_empty_dataframe():
    df = pd.DataFrame()
    assert not validate_data(df)
