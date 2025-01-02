import schedule
import time
import subprocess

def run_streams():
    print("Running streams.py")
    subprocess.run(["python", "streams.py"])

def run_country():
    print("Running country.py")
    subprocess.run(["python", "country.py"])

def run_sales_stores():
    print("Running sales_stores.py")
    subprocess.run(["python", "sales-stores.py"])

def run_sales_month():
    print("Running sales_month.py")
    subprocess.run(["python", "sales-month.py"])

def run_sales_country():
    print("Running sales_country.py")
    subprocess.run(["python", "sales-country.py"])

# Schedule tasks
schedule.every(24).hours.do(run_streams)
schedule.every(24).hours.do(run_country)
schedule.every(10).days.do(run_sales_stores)
schedule.every(10).days.do(run_sales_month)
schedule.every(10).days.do(run_sales_country)

if __name__ == "__main__":
    print("Scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(1)
