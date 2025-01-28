import schedule
import time
import subprocess

from streams import fetch_and_insert_data_store_streams, fetch_and_insert_data_country_streams

# Define functions for running sales scripts
def run_sales_scripts():
    run_sales_stores()
    run_sales_month()
    run_sales_country()

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

# Define function to run data streams
def run_streams():
    fetch_and_insert_data_store_streams()
    fetch_and_insert_data_country_streams()

# Schedule tasks
schedule.every(24).hours.do(run_streams)  # Run streams every 24 hours
schedule.every(15).days.do(run_sales_scripts)  # Run sales scripts every 15 days (twice a month)

if __name__ == "__main__":
    # Run streams immediately on startup
    run_streams()

    # Run sales scripts immediately on startup
    run_sales_scripts()

    # Start the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)
