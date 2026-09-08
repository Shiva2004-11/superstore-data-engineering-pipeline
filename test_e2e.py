import requests
from requests.auth import HTTPBasicAuth
import time

FLASK_API_URL = "http://localhost:5000/api/sales"
AIRFLOW_API_URL = "http://localhost:8080/api/v1"
AIRFLOW_USER = "airflow"
AIRFLOW_PASSWORD = "airflow"
DAG_ID = "superstore_etl_pipeline"

new_record = {
    "Row ID": 10000,
    "Order ID": "CA-2026-10000",
    "Customer ID": "CG-99999",
    "Customer Name": "Pipeline Test Customer",
    "Order Date": "2026-08-12",
    "Ship Date": "2026-08-12",
    "Ship Mode": "Standard Class",
    "Product ID": "TEC-TEST-001",
    "Sales": 500,
    "Quantity": 5,
    "Discount": 0.10,
    "Profit": 100,
    "Region": "West",
    "Segment": "Consumer",
    "Category": "Technology",
    "Sub-Category": "Accessories",
    "Country": "United States",
    "City": "New York City",
    "State": "New York",
    "Postal Code": 64105,
    "Product Name": "Pipeline Test Product"
}

# 1. Send to Flask
print("Sending to Flask...")
flask_response = requests.post(FLASK_API_URL, json=new_record)
if flask_response.status_code == 201:
    print("Flask Accepted.")
    
    # 2. Trigger Airflow
    print("Triggering Airflow...")
    data = {"conf": {}}
    response = requests.post(
        f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns",
        auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD),
        json=data
    )
    if response.status_code in (200, 201):
        run_id = response.json()['dag_run_id']
        print(f"Airflow Triggered! Run ID: {run_id}")
        
        # 3. Poll Airflow
        for i in range(30):
            res = requests.get(
                f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns/{run_id}",
                auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD)
            )
            if res.status_code == 200:
                state = res.json().get('state')
                print(f"State: {state}")
                if state in ('success', 'failed'):
                    break
            time.sleep(2)
    else:
        print(f"Failed to trigger Airflow: {response.text}")
else:
    print(f"Failed to send to Flask: {flask_response.text}")
