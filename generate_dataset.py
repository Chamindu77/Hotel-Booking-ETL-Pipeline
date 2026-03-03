import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
np.random.seed(42)

CATEGORIES = ['Luxury', 'Budget', 'Business', 'Resort', 'Boutique', 'Hostel']
COUNTRIES = ['USA', 'UK', 'France', 'Germany', 'Japan', 'Australia', 'UAE', 'india', 'FRANCE', 'usa']

def generate_hotel_data(n=12000):
    records = []
    for i in range(1, n + 1):

        record = {
            'id': i if random.random() > 0.02 else i, 
            'name': fake.company() if random.random() > 0.05 else None,
            'category': random.choice(CATEGORIES) if random.random() > 0.05 else random.choice(['luxury', 'BUDGET', '']),
            'price': round(random.uniform(30, 2000), 2) if random.random() > 0.05 else None,
            'rating': round(random.uniform(1, 5), 1) if random.random() > 0.05 else random.choice([99, -1, None]),
            'country': random.choice(COUNTRIES),
            'created_date': fake.date_between(start_date='-3y', end_date='today') if random.random() > 0.05 else fake.date_time().strftime('%d/%m/%Y'),
            'rooms_available': random.randint(1, 500) if random.random() > 0.05 else None,
            'reviews_count': random.randint(0, 5000),
        }
        records.append(record)

    records += random.choices(records[:500], k=300)

    df = pd.DataFrame(records)
    df.to_csv('data/raw/hotels_raw.csv', index=False)
    print(f"Generated {len(df)} records")

generate_hotel_data() 



