import schedule
import time
import subprocess
from datetime import datetime

def log_debug(message):
    print(f"{datetime.now()} - {message}")

def run_streams():
    try:
        subprocess.run(["python", "streams.py"], check=True)
    except subprocess.CalledProcessError as e:
        pass

def run_country():
    try:
        subprocess.run(["python", "country.py"], check=True)
    except subprocess.CalledProcessError as e:
        pass

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
        time.sleep(1)
