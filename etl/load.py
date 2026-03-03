import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from etl.logger import logger

load_dotenv()

def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)

def load_to_postgres(df: pd.DataFrame):
    engine = get_engine()
    df.to_sql('hotels', engine, if_exists='append', index=False, schema='public')
    logger.info(f"Loaded {len(df)} records into PostgreSQL")

def save_rejected(df: pd.DataFrame):
    if not df.empty:
        df.to_csv('data/cleaned/rejected_records.csv', index=False)
        logger.info(f"Saved {len(df)} rejected records to CSV")