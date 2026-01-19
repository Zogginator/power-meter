import os
import logging
import json

from pathlib import Path
from datetime import datetime, time, timezone, timedelta

from ingest.logging_setup import setup_console
from ingest.eon.client import EonClient, EonQuery, TokenStore, MeasurementSeries, MeasurementPoint
from ingest.influx.writer import write_series
from ingest.influx.reader import last_ts_with_data, daily_datapoints
from ingest.jobs.loader import load_meas

TOKEN_PATH = Path(os.environ["EON_TOKEN_PATH"])
SOURCE_CONFIG_PATH = Path(os.environ["EON_SOURCE_CONFIG_PATH"])
ALERT_THRESHOLD = 5
MAX_QUERY_DAYS = 30   #days

log = logging.getLogger(__name__)

def run_adhoc ():
    load_meas(datetime(2026,1,14), datetime(2026,1,15))


def run_daily():
    log.info("Daily run starts")
    
    with open(SOURCE_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if last_ts_with_data():
        lts_day= last_ts_with_data().replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        lts_day = today
        log.info("There is no data in the DB. Ingest starts with yesterday day.")
    
   
    print(f"Last timestamp with data:", lts_day,"  datapoints: ", daily_datapoints(lts_day))
    print(lts_day, today)
    delta_days = (today - lts_day).days 
   
    print("Delta: ", delta_days)
    if delta_days >=ALERT_THRESHOLD:
        print("Threshold Alert to be sent")       #send_alert_mail("To many failed days")
    if delta_days >= MAX_QUERY_DAYS:
        raise Exception("Too many days to query")
    

    start_date = lts_day + timedelta(days=-1)
    end_date = today
    
    load_meas(start_date, end_date)

    return None

if __name__ == "__main__":
    run_daily()
    #run_adhoc()
