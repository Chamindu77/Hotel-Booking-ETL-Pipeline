import pandas as pd
import numpy as np
from etl.logger import logger

def clean_data(df: pd.DataFrame):
    initial_count = len(df)
    rejected = []

    # Remove duplicates
    df = df.drop_duplicates(subset=['id'])
    logger.info(f"Removed {initial_count - len(df)} duplicates")

    # Standardize text columns
    df['category'] = df['category'].str.strip().str.title()
    df['country'] = df['country'].str.strip().str.upper()
    df['name'] = df['name'].str.strip()

    # Fix dates — handle multiple formats
    def parse_date(val):
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y'):
            try:
                return pd.to_datetime(val, format=fmt)
            except:
                pass
        return pd.NaT

    df['created_date'] = df['created_date'].apply(parse_date)

    # Validate & reject bad records
    valid_categories = ['Luxury', 'Budget', 'Business', 'Resort', 'Boutique', 'Hostel']

    def is_valid(row):
        reasons = []
        if pd.isna(row['name']) or row['name'] == '':
            reasons.append('Missing name')
        if pd.isna(row['price']) or row['price'] <= 0:
            reasons.append('Invalid price')
        if pd.isna(row['rating']) or not (1 <= row['rating'] <= 5):
            reasons.append('Invalid rating')
        if row['category'] not in valid_categories:
            reasons.append('Invalid category')
        if pd.isna(row['created_date']):
            reasons.append('Invalid date')
        return reasons

    rejected_rows = []
    valid_rows = []
    for _, row in df.iterrows():
        reasons = is_valid(row)
        if reasons:
            row_dict = row.to_dict()
            row_dict['rejection_reason'] = '; '.join(reasons)
            rejected_rows.append(row_dict)
            logger.warning(f"REJECTED record id={row['id']}: {reasons}")
        else:
            valid_rows.append(row)

    rejected_df = pd.DataFrame(rejected_rows)
    clean_df = pd.DataFrame(valid_rows)

    # Fill remaining nulls
    clean_df['rooms_available'] = clean_df['rooms_available'].fillna(clean_df['rooms_available'].median())
    clean_df['reviews_count'] = clean_df['reviews_count'].fillna(0)

    logger.info(f"Clean records: {len(clean_df)} | Rejected: {len(rejected_df)}")
    return clean_df, rejected_df