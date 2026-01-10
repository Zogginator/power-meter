from ingest.lock import acquire_lock
from ingest.jobs.daily import run_daily

def main():
    acquire_lock()
    run_daily()

if __name__ == "__main__":
    main()
