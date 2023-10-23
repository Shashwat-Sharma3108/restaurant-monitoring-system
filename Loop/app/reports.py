import pandas as pd

from pytz import timezone as pytz_timezone
from datetime import timedelta
from loguru import logger
from multiprocessing.dummy import Pool as ThreadPool

from models import *
from database import *

def create_report(report_id):
    """
    The function `create_report` retrieves data from multiple stores, logs information about the
    process, and saves the data as a CSV file.
    """
    try:
        stores = get_stores()

        logger.info(f"GOT TOTAL STORES {len(stores)}")
        csv_data = []

        # Can be done using threadpool when in Production database 
        # pool = ThreadPool(10)
        # results = pool.map(get_report_data, stores)
        # pool.close()
        # pool.join()
        
        for store in stores[:250]:
            data = get_report_data(store)
            csv_data.append(data)

        required_df = pd.DataFrame(csv_data)
        required_df.to_csv(f'../Output_CSV/{report_id}.csv',index=False)

        with get_db() as db:
            data = db.query(Report).filter(
                Report.report_id == report_id
            ).first()
        
            if data:
                data.completed_at = datetime.now()
                data.status = 'FINISHED'

                db.commit()
            else:
                raise Exception(f"NO REPORT FOUND FOR  : {report_id}")
    except Exception as e:
        logger.info(f"EXCEPTION WHILE CREATING REPORT : {e}")
        with get_db() as db:
            data = db.query(Report).filter(
                Report.report_id == report_id
            ).first()
        
        if data:
            data.completed_at = datetime.now()
            data.status = 'ERROR'
            db.commit()

        logger.error(f"ERROR WHILE FETCHING DATA : {e}")


def get_report_data(store:TimeZone) -> dict:
    """
    The function `get_report_data` retrieves data from a database based on the store's time zone and
    returns a dictionary containing information for the last one hour, one day, and one week.
    
    :param store: The `store` parameter is of type `TimeZone`
    :type store: TimeZone
    :return: a dictionary containing various data related to the store. The dictionary includes the
    store ID and data for the last one hour, last one day, and last one week.
    """
    logger.info(f"WORKING IN STORE ID {store.store_id}")
    stores_time_zone = store.timezone_str or 'America/Chicago'
    target_timezone = pytz_timezone(stores_time_zone)

    with get_db() as db:
        required_time = db.query(Store.timestamp_utc).order_by(Store.timestamp_utc.desc()).first().timestamp_utc

    local_time = required_time.astimezone(target_timezone)
    utc_timezone = pytz_timezone('UTC')
    utc_time = required_time.astimezone(utc_timezone)

    current_day = local_time.weekday()
    current_time = local_time.time()

    # last one hour 
    last_one_hour_data = get_last_one_hour_data(store, utc_time, current_day, current_time)
    # last one day
    last_one_day_data = get_last_one_day_data(store, utc_time, current_day, current_time)
    # last one week
    last_one_week_data = get_last_one_week_data(store, utc_time, current_day, current_time)

    required_dict = {'store_id':store.store_id,  **last_one_hour_data, **last_one_day_data, **last_one_week_data}
    return required_dict
    
def get_last_one_hour_data(store: TimeZone, utc_time:DateTime, current_day: int, current_time: DateTime) -> dict:
    """
    The function `get_last_one_hour_data` retrieves the last hour's uptime and downtime data for a given
    store based on its business hours and logs.
    
    :param store: The `store` parameter is of type `TimeZone`, which likely represents a specific store
    or location
    :type store: TimeZone
    :param utc_time: The `utc_time` parameter is a `DateTime` object representing the current UTC time
    :type utc_time: DateTime
    :param current_day: The current day of the week, represented as an integer. For example, Monday is
    represented as 0, Tuesday as 1, and so on
    :type current_day: int
    :param current_time: The current time is the time at which the function is being called. It is of
    type DateTime
    :type current_time: DateTime
    :return: a dictionary containing the last hour's uptime and downtime for a given store.
    """
    last_one_hour_data = {"last_hour_uptime":0, "last_hour_downtime":0}

    with get_db() as db:
        is_store_open = db.query(db.query(BusinessHours).filter(
            BusinessHours.store_id == store.store_id,
            BusinessHours.day_of_week ==current_day,
            BusinessHours.start_time_local <= current_time,
            BusinessHours.end_time_local >= current_time,
        ).exists()).scalar()

    if not is_store_open:
        return last_one_hour_data
    
    with get_db() as db:
        last_hour_logs = db.query(Store).filter(
            Store.store_id == store.store_id,
            Store.timestamp_utc >= utc_time - timedelta(hours=1)
        ).order_by(Store.timestamp_utc)

    if last_hour_logs.count() > 0:
        last_one_hour_status = last_hour_logs[0].status
        if last_one_hour_status.lower() == "active":
            last_one_hour_data["last_hour_uptime"] = 60
        else:
            last_one_hour_data["last_hour_downtime"] = 60

    return last_one_hour_data

def get_last_one_day_data(store, utc_time, current_day, current_time):
    last_one_day_data = {'last_one_day_uptime':0, 'last_one_day_downtime':0}

    one_day_ago = current_day - 1 if current_day > 0 else 6
    
    with get_db() as db:
        is_store_open_aday_ago = db.query(
            db.query(BusinessHours).filter(
                BusinessHours.store_id == store.store_id,
                BusinessHours.day_of_week >= one_day_ago,
                BusinessHours.day_of_week <= current_day,
                BusinessHours.start_time_local <= current_time,
                BusinessHours.end_time_local >= current_time
            ).exists()
        ).scalar()

    if not is_store_open_aday_ago:
        return last_one_day_data
    
    with get_db() as db:
        last_one_day_logs = db.query(Store).filter(
            Store.store_id == store.store_id,
            Store.timestamp_utc >= utc_time - timedelta(days=1)
        ).order_by(Store.timestamp_utc)
    
    for log in last_one_day_logs:
        log_in_business_hours = db.query(
            db.query(BusinessHours).filter(
                BusinessHours.day_of_week == log.timestamp_utc.weekday(),
                BusinessHours.start_time_local <= log.timestamp_utc.time(),
                BusinessHours.end_time_local >= log.timestamp_utc.time(),
            ).exists()
        ).scalar()

        if not log_in_business_hours:
            continue
        if log.status.lower() == "active":
            last_one_day_data['last_one_day_uptime'] += 1
        else:
            last_one_day_data['last_one_day_downtime'] += 1
    return last_one_day_data


def get_last_one_week_data(store, utc_time, current_day, current_time):
    last_one_week_data = {'last_week_uptime':0, 'last_week_downtime':0}

    one_week_ago = current_day - 7 if current_day > 0 else 0

    with get_db() as db:
        is_store_open_week_ago = db.query(
            db.query(BusinessHours).filter(
                BusinessHours.store_id == store.store_id,
                BusinessHours.day_of_week >= one_week_ago,
                BusinessHours.day_of_week <= current_day,
                BusinessHours.start_time_local <= current_time,
                BusinessHours.end_time_local >= current_time
            ).exists()
        ).scalar()

        if not is_store_open_week_ago:
            return last_one_week_data
    
        last_one_week_logs = db.query(Store).filter(
            Store.store_id == store.store_id,
            Store.timestamp_utc >= utc_time - timedelta(days=7)
        ).order_by(Store.timestamp_utc)

    for log in last_one_week_logs:
        log_in_business_hours = db.query(
            db.query(BusinessHours).filter(
                BusinessHours.day_of_week == log.timestamp_utc.weekday(),
                BusinessHours.start_time_local <= log.timestamp_utc.time(),
                BusinessHours.end_time_local >= log.timestamp_utc.time(),
            ).exists()
        ).scalar()

        if not log_in_business_hours:
            continue
        if log.status.lower() == "active":
            last_one_week_data['last_week_uptime'] += 1
        else:
            last_one_week_data['last_week_downtime'] += 1
    return last_one_week_data

def get_stores():
    with get_db() as db:
        data = db.query(TimeZone.store_id, TimeZone.timezone_str).distinct(TimeZone.store_id, TimeZone.timezone_str).all()
        return data
    
if __name__ == "__main__":
    create_report()
