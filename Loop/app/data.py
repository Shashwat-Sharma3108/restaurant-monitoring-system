import pandas as pd
import pytz

from models import *
from database import Base, engine, SessionLocal

def get_data():
    db = SessionLocal()
    read_size = 100
    Base.metadata.create_all(engine)

    #reading data from CSV'S
    stores_csv = pd.read_csv('../CSV/stores.csv', chunksize=read_size)
    business_hours_csv = pd.read_csv('../CSV/business_hours.csv', chunksize=read_size)
    time_zones_csv = pd.read_csv('../CSV/timezones.csv', chunksize=read_size)
    
    timezone_dict = {}

    for _, data in pd.concat(time_zones_csv).iterrows():
        timezone_dict[data.get('store_id')] = pytz.timezone(data.get('timezone_str'))

    for store_data in stores_csv:
        required_df = store_data.dropna(subset=['timestamp_utc'])
        required_df['timestamp_utc'] = pd.to_datetime(required_df['timestamp_utc'])

        for _, row in required_df.iterrows():
            store_id = row.get('store_id')
            status = row.get('status')
            timezone = timezone_dict.get(store_id, pytz.timezone('America/Chicago'))
            local_timestamp = row['timestamp_utc'].astimezone(timezone)
            store = Store(store_id = store_id,timestamp_utc = row.get('timestamp_utc'), status=status)
            db.add(store)
            if (_+1) % read_size == 0:
                db.commit()

        db.commit()       
    print(f"Number of Stores : {len(db.query(Store).all())}")

    for bussiness_hour_data in business_hours_csv:
        for i, row in bussiness_hour_data.iterrows():
            start_time = pd.to_datetime(row['start_time_local']).time()
            end_time = pd.to_datetime(row['end_time_local']).time()
            business_hours = BusinessHours(store_id=row['store_id'], day_of_week=row['day'], start_time_local=start_time, end_time_local=end_time)
            db.add(business_hours)
            if (i+1) % read_size == 0:
                    db.commit()
        db.commit()
    print(f"Total Business hours Entries : {len(db.query(BusinessHours).all())}")

    for i, timezone_data in enumerate(timezone_dict.items()):
        timezone_instance = TimeZone(store_id=timezone_data[0], timezone_str = str(timezone_data[1]))
        db.add(timezone_instance)
        if (i+1) % read_size == 0:
                db.commit()
        db.commit()
    print(f"Total Timezones : {len(db.query(TimeZone).all())}")
    
    db.close()

if __name__ == "__main__":
    get_data()