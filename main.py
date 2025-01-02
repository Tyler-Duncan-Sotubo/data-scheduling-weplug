import schedule
import time
import subprocess
from datetime import datetime

def log_debug(message):
    print(f"{datetime.now()} - {message}")

def run_streams():
    log_debug("Starting streams.py")
    try:
        subprocess.run(["python", "streams.py"], check=True)
        log_debug("Finished running streams.py")
    except subprocess.CalledProcessError as e:
        log_debug(f"Error running streams.py: {e}")

def run_country():
    log_debug("Starting country.py")
    try:
        subprocess.run(["python", "country.py"], check=True)
        log_debug("Finished running country.py")
    except subprocess.CalledProcessError as e:
        log_debug(f"Error running country.py: {e}")

def run_sales_stores():
    log_debug("Starting sales_stores.py")
    try:
        subprocess.run(["python", "sales-stores.py"], check=True)
        log_debug("Finished running sales_stores.py")
    except subprocess.CalledProcessError as e:
        log_debug(f"Error running sales_stores.py: {e}")

def run_sales_month():
    log_debug("Starting sales_month.py")
    try:
        subprocess.run(["python", "sales-month.py"], check=True)
        log_debug("Finished running sales_month.py")
    except subprocess.CalledProcessError as e:
        log_debug(f"Error running sales_month.py: {e}")

def run_sales_country():
    log_debug("Starting sales_country.py")
    try:
        subprocess.run(["python", "sales-country.py"], check=True)
        log_debug("Finished running sales_country.py")
    except subprocess.CalledProcessError as e:
        log_debug(f"Error running sales_country.py: {e}")

# Schedule tasks
log_debug("Setting up schedules...")
schedule.every(24).hours.do(run_streams)
schedule.every(24).hours.do(run_country)
schedule.every(10).days.do(run_sales_stores)
schedule.every(10).days.do(run_sales_month)
schedule.every(10).days.do(run_sales_country)

if __name__ == "__main__":
    log_debug("Scheduler started.")
    while True:
        schedule.run_pending()
        log_debug("Checked pending tasks.")
        time.sleep(1)
