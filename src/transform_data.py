import pandas as pd


def data_cleaning(df, year, month):
    """
    Cleans the input DataFrame by performing the following operations:
    1. Selects relevant columns: 'tpep_pickup_datetime', 'tpep_dropoff_datetime', and 'trip_distance'.
    2. Calculates the trip duration in minutes and adds it as a new column 'trip_duration'.
    3. Filters out rows where 'trip_duration' or 'trip_distance' is less than or equal to zero.
    4. Removes data that does not match the specified year and month.
    Args:
        df (pandas.DataFrame): The input DataFrame containing trip data.
        year (int): The year to filter the data.
        month (int): The month to filter the data.
    Returns:
        pandas.DataFrame: The cleaned DataFrame.
    """

    df = df[["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]].copy()
    df.loc[:, "trip_duration"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60
    df = df[(df["trip_duration"] > 0) & (df["trip_distance"] > 0)]
    df = remove_different_month_data(df, year, month)

    return df


def remove_different_month_data(df, year, month):
    """
    Filters the DataFrame to include only rows where the 'tpep_pickup_datetime' column
    matches the specified year and month.

    Args:
        df (pandas.DataFrame): The input DataFrame containing a 'tpep_pickup_datetime' column.
        year (int): The year to filter by.
        month (int): The month to filter by.

    Returns:
        pandas.DataFrame: A DataFrame containing only the rows where the 'tpep_pickup_datetime'
        matches the specified year and month.
    """
    df = df[
        (df["tpep_pickup_datetime"].dt.month == month)
        & (df["tpep_pickup_datetime"].dt.year == year)
    ]
    return df


def calculate_monthly_average(df, month):
    """
    Calculate the monthly average of trip distances.

    Args:
        df (pandas.DataFrame): DataFrame containing trip data with a 'trip_distance' column.
        month (str): The month for which the average is being calculated.

    Returns:
        pandas.DataFrame: A DataFrame with the month and the calculated monthly average of trip distances.
    """

    monthly_avg = df["trip_distance"].mean()
    result_df = pd.DataFrame({"month": [month], "monthly_average": [monthly_avg]})
    return result_df


def calculate_rolling_average(df, window_size):
    """
    Calculate the rolling average of trip distances in a DataFrame.
    This function sorts the DataFrame by the 'tpep_pickup_datetime' column and then calculates
    the rolling average of the 'trip_distance' column over a specified window size. The result
    is stored in a new column named 'rolling_avg_distance'.
    Parameters:
    df (pandas.DataFrame): The input DataFrame containing trip data.
    window_size (int): The size of the rolling window to calculate the average.
    Returns:
    pandas.DataFrame: The DataFrame with an additional column 'rolling_avg_distance' containing
                      the rolling average of trip distances.
    """

    df = df.sort_values(by="tpep_pickup_datetime")
    df["rolling_avg_distance"] = df["trip_distance"].rolling(window=window_size).mean()

    return df
