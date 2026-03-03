import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from etl.logger import logger

load_dotenv()

def extract_from_local(path='data/raw/hotels_raw.csv'):
    logger.info(f"Extracting data from {path}")
    df = pd.read_csv(path)
    logger.info(f"Extracted {len(df)} raw records")
    return df

def upload_to_s3(filepath, s3_key):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        bucket = os.getenv('S3_BUCKET_NAME')
        s3.upload_file(filepath, bucket, s3_key)
        logger.info(f"Uploaded {filepath} to s3://{bucket}/{s3_key}")
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")