import schedule
import time
import subprocess
from datetime import datetime
from streams import fetch_and_insert_data_store_streams
from country import fetch_and_insert_data_country_streams

def run_streams():
    fetch_and_insert_data_store_streams()
    fetch_and_insert_data_country_streams()

def run_sales_stores():
    try:
        subprocess.run(["python", "sales-stores.py"], check=True)
    except subprocess.CalledProcessError as e:
        pass

def run_sales_month():
    try:
        subprocess.run(["python", "sales-month.py"], check=True)
    except subprocess.CalledProcessError as e:
        pass

def run_sales_country():
    try:
        subprocess.run(["python", "sales-country.py"], check=True)
    except subprocess.CalledProcessError as e:
        pass

# Scheduling the functions
schedule.every(24).hours.do(run_streams)  # Run streams every 24 hours
schedule.every(15).days.do(run_sales_stores)  # Run sales-stores every 15 days (twice a month)
schedule.every(15).days.do(run_sales_month)  # Run sales-month every 15 days (twice a month)
schedule.every(15).days.do(run_sales_country)  # Run sales-country every 15 days (twice a month)

if __name__ == "__main__":
    # Run the first execution immediately on startup
    run_streams()

    # Start the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)
