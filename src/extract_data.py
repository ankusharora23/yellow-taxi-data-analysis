import requests
import os
import pandas as pd
import calendar
import math
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src import transform_data


def download_data(year, month, file_location="data/input/"):
    """
    Downloads yellow taxi trip data for a specified year and month from a remote server and saves it as a parquet file.
    Args:
        year (int): The year of the data to download.
        month (int): The month of the data to download.
    Returns:
        str: The file path of the downloaded parquet file.
    Raises:
        Exception: If the data download fails with a status code other than 200.
    """

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02}.parquet"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to download data: {response.status_code}")
    file_name = f"{file_location}yellow_tripdata_{year}-{month:02}.parquet"
    with open(file_name, "wb") as file:
        file.write(response.content)
    return file_name


def extract_to_df(file_name):
    df = pd.read_parquet(file_name)
    return df


def num_previous_file_needed(year, month, window_size):
    """
    Calculate the number of previous files needed based on the given year, month, and window size.

    Args:
        year (int): The year for which the calculation is being made.
        month (int): The month for which the calculation is being made.
        window_size (int): The size of the window in days.

    Returns:
        int: The number of previous files needed.
    """
    number_of_days_in_month = calendar.monthrange(year, month)[1]
    num_previous_file_needed = math.ceil(window_size / number_of_days_in_month)
    return num_previous_file_needed


def get_previous_months(year, month, window_size):
    """
    Retrieves and processes data for the specified number of previous months.
    Args:
        year (int): The year of the starting month.
        month (int): The month of the starting year.
        window_size (int): The number of previous months to retrieve data for.
    Returns:
        pd.DataFrame: A concatenated DataFrame containing cleaned data for the specified previous months.
    This function calculates the required number of previous months based on the given window size,
    downloads the data for each of those months, extracts it into DataFrames, cleans the data, and
    concatenates all the cleaned DataFrames into a single DataFrame.
    """
    # Create a datetime object for the given year and month
    previous_file_needed = num_previous_file_needed(year, month, window_size)

    # downloads and concatenate data for previous months

    all_data = []
    for i in range(1, previous_file_needed + 1):
        previous_date = datetime(year, month, 1) - relativedelta(months=i)
        file_name = download_data(previous_date.year, previous_date.month)
        extracted_df = extract_to_df(file_name)
        cleaned_df = transform_data.data_cleaning(
            extracted_df, previous_date.year, previous_date.month
        )
        all_data.append(cleaned_df)
    concatenated_data = pd.concat(all_data, ignore_index=True)
    return concatenated_data
