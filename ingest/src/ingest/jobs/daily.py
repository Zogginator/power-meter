import os
import logging
import json

from pathlib import Path
from datetime import datetime, time, timezone, timedelta

from ingest.logging_setup import setup_logging
from ingest.eon.client import EonClient, EonQuery, TokenStore, MeasurementSeries, MeasurementPoint
from ingest.influx.writer import write_series
from ingest.influx.reader import last_ts_with_data, daily_datapoints
from ingest.jobs.loader import load_meas

TOKEN_PATH = Path(os.environ["EON_TOKEN_PATH"])
SOURCE_CONFIG_PATH = Path(os.environ["EON_SOURCE_CONFIG_PATH"])
ALERT_THRESHOLD = 5
MAX_QUERY_DAYS = 5   #days, normally 30

log = logging.getLogger("daily")

def run_adhoc ():
    load_meas(datetime(2023,5,10), datetime(2023,6,9))


def run_daily():
    log.info("Daily run starts")
    
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    lts_day= last_ts_with_data()
    
    if lts_day:
        lts_day= lts_day.replace(hour=0, minute=0, second=0, microsecond=0)   # last day when time stamp exists ie. data entered
    else:
        lts_day = today
        log.info("There is no data in the DB. Ingest starts with yesterday.")
    

    delta_days = (today - lts_day).days 
   
    log.info("Delta: %i", delta_days)
    if delta_days >=ALERT_THRESHOLD:
        print("Threshold Alert to be sent")       #send_alert_mail("To many failed days")
    if delta_days >= MAX_QUERY_DAYS:
        start_date = today - timedelta(days = MAX_QUERY_DAYS) 
        log.info("Too many days to query. Start date is set to: %s", start_date)
    else:
        start_date = lts_day + timedelta(days=-1)
    
    end_date = today
    
    load_meas(start_date, end_date)

    return None

if __name__ == "__main__":
    setup_logging()
    run_daily()
    #run_adhoc()
