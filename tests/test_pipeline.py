import pytest
import pandas as pd
from src import extract_data, transform_data, load_data
import sqlite3

# downloading parquet file to test throught the process
extract_data.download_data(year=2024, month=2, file_location=r'tests/data/input/')

def test_num_previous_file_needed():
   
   num_previous_file_needed = extract_data.num_previous_file_needed(year=2024, month=2, window_size=45)
   assert num_previous_file_needed==2

   num_previous_file_needed = extract_data.num_previous_file_needed(year=2024, month=12, window_size=28)
   assert num_previous_file_needed==1

def test_get_previous_months():
    pass


def test_data_cleaning():
    pass

def test_calculate_monthly_average():
    pass

def calculate_rolling_average():
    pass

def run_pipeline():
    pass
