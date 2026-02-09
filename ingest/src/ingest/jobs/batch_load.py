import os
import logging
import argparse

from pathlib import Path
from datetime import datetime, date, timezone, timedelta

from ingest.logging_setup import setup_logging
from ingest.influx.reader import last_ts_with_data
from ingest.jobs.loader import load_meas

TOKEN_PATH = Path(os.environ["EON_TOKEN_PATH"])
SOURCE_CONFIG_PATH = Path(os.environ["EON_SOURCE_CONFIG_PATH"])
ALERT_THRESHOLD = 5
MAX_QUERY_DAYS = 30   #days, normally 30

log = logging.getLogger("batch_loader")

def parse_args():
    p = argparse.ArgumentParser(description="Measurements batch load")
    p.add_argument(
        "-s", "--start",
        required = True, 
        type=date.fromisoformat,
        help="Start date of the upload in ISO format (YYYY-MM-DD)",
    )
    p.add_argument(
        "-e", "--end",
        type=date.fromisoformat,
        default= date.today(),
        help="End date of the upload in ISO format (YYYY-MM-DD), default = today",
    )
    p.add_argument(
        "-p", "--period",
        type= int,
        default = 30, 
        help="Number of days in an upload period (Max and default = 30)",
    )
    
    return p.parse_args()

def run_adhoc ():
    load_meas(datetime(2023,5,10), datetime(2023,6,9))


def run_batch():
    setup_logging()
    args=parse_args()


    if args.period > 30: 
        raise ValueError("Too long period: must be max. 30 days.")
    
    if args.start > args.end:
        raise ValueError("Start date must be before end date.")
    
    log.info("Batch run starts")
    log.info("Start date: %s, end date: %s, period: %s days", args.start, args.end, args.period)

    today = date.today()
    start_date = args.start
    eff_end =min(args.end, today)
    
    while True:
        #the end has reached
        if start_date > eff_end:
            break
        
        end_date = start_date+timedelta(days=args.period-1)
        
        #do not go past requested end
        if end_date >= eff_end:
            end_date = eff_end
        
        if end_date < start_date:
            break

        #print (start_date, end_date)
        load_meas(start_date, end_date)    #fetch and load measurements

        #kill endless loop
        if end_date == eff_end:
            break


        start_date=end_date+timedelta(days=1)



if __name__ == "__main__":
    raise SystemExit(run_batch())


