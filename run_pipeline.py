from etl.extract import extract_from_local, upload_to_s3
from etl.transform import clean_data
from etl.load import load_to_postgres, save_rejected
from etl.logger import logger
import pandas as pd

def run():
    logger.info("=== ETL Pipeline Started ===")

    # EXTRACT
    raw_df = extract_from_local('data/raw/hotels_raw.csv')
    upload_to_s3('data/raw/hotels_raw.csv', 'raw/hotels_raw.csv') 

    # TRANSFORM
    clean_df, rejected_df = clean_data(raw_df)

    # Save cleaned to CSV and S3
    clean_df.to_csv('data/cleaned/hotels_clean.csv', index=False)
    upload_to_s3('data/cleaned/hotels_clean.csv', 'cleaned/hotels_clean.csv')

    # LOAD
    save_rejected(rejected_df)
    load_to_postgres(clean_df)

    logger.info("=== ETL Pipeline Completed ===")

if __name__ == '__main__':
    run()