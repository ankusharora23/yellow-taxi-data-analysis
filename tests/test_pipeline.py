import pytest
import pandas as pd
from src import extract_data, transform_data, load_data
import sqlite3
import requests
import requests_mock
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Preparing test environement
def initialisation():

    date = [{"year": 2024, "month": 2}, {"year": 2024, "month": 1}]
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    for d in date:
        year = d["year"]
        month = d["month"]
        file_name = f"yellow_tripdata_{year}-{month}.parquet"
        if not os.path.exists(f"tests/data/input{file_name}"):

            extract_data.download_data(
                year, month, url, file_location="tests/data/input"
            )

# Initialising test environment
year = 2024
month = 2
window_size = 20
file_name = f"yellow_tripdata_{year}-{month:02}.parquet"
url = f"https://dummy.cloudfront.net/trip-data/"
initialisation()


def load_dummy_data(file_name):

    dummy_data_path = f"tests/data/input/{file_name}"
    with open(dummy_data_path, "rb") as f:
        dummy_data = f.read()
    return dummy_data


@pytest.fixture
def mock_requests():
    urls = [
        "https://dummy.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
        "https://dummy.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
        # Add more URLs as needed
    ]
    file_names = [
        "yellow_tripdata_2024-01.parquet",
        "yellow_tripdata_2024-02.parquet",
        # Add corresponding file names
    ]

    with requests_mock.Mocker() as m:
        for url, file_name in zip(urls, file_names):
            dummy_data = load_dummy_data(file_name)
            m.get(url, content=dummy_data)
        yield m


def test_num_previous_file_needed():

    num_previous_file_needed = extract_data.num_previous_file_needed(
        year=2024, month=2, window_size=45
    )
    assert num_previous_file_needed == 2

    num_previous_file_needed = extract_data.num_previous_file_needed(
        year=2024, month=12, window_size=28
    )
    assert num_previous_file_needed == 1


def test_get_previous_months(mock_requests):
    window_size = 20
    df = extract_data.get_previous_months(year, month, window_size, url)
    assert len(df) == 2904123


def test_data_cleaning():
    df = pd.read_parquet(f"tests/data/input/yellow_tripdata_{year}-{month:02}.parquet")
    transform_data.data_cleaning(df, year, month)


def test_calculate_monthly_average():
    pass


def calculate_rolling_average():
    pass


def run_pipeline():
    pass


def test_download_data(mock_requests):

    response = extract_data.download_data(
        year=year, month=month, url=url, file_location="tests/data/input"
    )

    assert response["status"] == 200
