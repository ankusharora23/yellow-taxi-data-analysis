import argparse
from src import extract_data, transform_data, load_data
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_pipeline(args):

    logging.info("Starting the pipeline")
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    file_name = extract_data.download_data(args.year, args.month, url)["file_name"]
    logging.info(f"Downloaded data for {args.year}-{args.month} to {file_name}")

    df = extract_data.extract_to_df(file_name)
    logging.info(f"Extracted data to DataFrame with {len(df)} records")

    df = transform_data.data_cleaning(df, args.year, args.month)
    logging.info(f"Data cleaned and number of records is {len(df)}")

    monthly_avg = transform_data.calculate_monthly_average(df, args.year, args.month)
    logging.info(f"Calculated monthly average trip lengths:{monthly_avg} ")

    load_data.store_results(monthly_avg, args.db_name, "monthly_avg_trip_length")
    logging.info(
        "Stored monthly average trip lengths in database with table name monthly_avg_trip_length"
    )

    concat_all_previous_data = extract_data.get_previous_months(
        args.year, args.month, args.window_size, url
    )
    logging.info("Retrieved previous months' data")

    concat_all_data = pd.concat([df, concat_all_previous_data], ignore_index=True)
    rolling_avg = transform_data.calculate_rolling_average(
        concat_all_data, args.window_size
    )
    logging.info("Concatenated current and previous months' data")

    rolling_avg = transform_data.remove_different_month_data(
        rolling_avg, args.year, args.month
    )
    logging.info("Calculated rolling average")

    load_data.store_results(rolling_avg, args.db_name, "yellow_taxi_data")
    logging.info(
        "Stored rolling average data in database with table name yellow_taxi_data"
    )

    logging.info("Pipeline finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi Data Pipeline")
    parser.add_argument(
        "--year", type=int, required=True, help="Year of the data to process"
    )
    parser.add_argument(
        "--month", type=int, required=True, help="Month of the data to process"
    )
    parser.add_argument(
        "--window-size",
        type=int,
        required=False,
        default=10,
        help="Window size for calculating rolling averages",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        required=False,
        default="sqlitedb",
        help="Name of the database to store results",
    )
    args = parser.parse_args()

    # Run the pipeline with the provided arguments
    run_pipeline(args)
