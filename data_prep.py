# API for Data Prep: https://api.trifacta.com/dataprep-enterprise-cloud/index.html

import json
import requests
import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Constants
PROJECT_ID = "intrepid-signal-310513"  # Aligned with big_query.py
DATASET_ID = "ecommerce"
TABLE_ID = "all_sessions_raw_dataprep"
OUTPUT_TABLE = "revenue_reporting"
CREDENTIALS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "authentication", "credentials.json")

# Global clients
BIG_QUERY_CLIENT = None

def main():
    """Main function to orchestrate the entire process."""
    print("Welcome to the Data Prep API Examples project!")
    
    # Authenticate with GCP
    authenticate_with_gcp()
    
    # Step 1: Test connection to Cloud Dataprep API
    print("\nStep 1: Testing connection to Cloud Dataprep API...")
    if not test_dataprep_connection():
        print("Failed to connect to Cloud Dataprep API. Exiting.")
        return
    
    # Step 2: Create BigQuery dataset
    print("\nStep 2: Creating BigQuery dataset...")
    if not create_bigquery_dataset():
        print("Failed to create BigQuery dataset. Exiting.")
        return
    
    # Step 3: Connect BigQuery data to Cloud Dataprep
    auth_token = get_token()
    print("\nStep 3: Connecting BigQuery data to Cloud Dataprep...")
    connection_details = connect_bigquery_to_dataprep(auth_token)
    if not connection_details:
        print("Failed to connect BigQuery data to Cloud Dataprep. Exiting.")
        return
    
    print("\nAll steps completed successfully!")

def authenticate_with_gcp():
    """Authenticate with Google Cloud Platform using service account credentials."""
    global BIG_QUERY_CLIENT
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        BIG_QUERY_CLIENT = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        print(f"Successfully authenticated with GCP project: {PROJECT_ID}")
        return True
    except Exception as e:
        print(f"Error authenticating with GCP: {e}")
        return False

def get_token():
    """Retrieve the Dataprep API token from dataprep_token.json."""
    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "authentication/dataprep_token.json")
    try:
        with open(token_path, "r") as token_file:
            token_data = json.load(token_file)
            auth_token = token_data.get("dataprep_token")
            if not auth_token:
                raise ValueError("Token not found in dataprep_token.json")
            return auth_token
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading token file: {e}")
        return None

def get_proxies():
    """Retrieve proxy settings from environment variables."""
    http_proxy = os.getenv("HTTP_PROXY")
    https_proxy = os.getenv("HTTPS_PROXY")

    if http_proxy or https_proxy:
        return {
            "http": http_proxy,
            "https": https_proxy
        }
    return None

def test_dataprep_connection():
    """Test the connection to the Cloud Dataprep API."""
    auth_token = get_token()
    if not auth_token:
        return False

    url = "https://api.clouddataprep.com/v4/open-api-spec"
    headers = {
        "Authorization": f"Bearer {auth_token}"
    }

    proxies = get_proxies()
    response = requests.get(url, headers=headers, proxies=proxies)

    # Check the response
    if response.status_code == 200:
        print("Connection to Cloud Dataprep API successful.")
        return True
    else:
        print("Failed to connect to Cloud Dataprep API.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return False

def create_bigquery_dataset():
    """Step 2: Create a BigQuery dataset - implements Task 2 of the lab."""
    try:
        global BIG_QUERY_CLIENT
        if BIG_QUERY_CLIENT is None:
            authenticate_with_gcp()
            
        if BIG_QUERY_CLIENT is None:
            print("BigQuery client not initialized. Authentication failed.")
            return False
            
        # Create dataset
        dataset_ref = BIG_QUERY_CLIENT.dataset(DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        
        # Check if dataset exists
        try:
            BIG_QUERY_CLIENT.get_dataset(dataset_ref)
            print(f"Dataset {DATASET_ID} already exists.")
        except Exception:
            # Create dataset if it doesn't exist
            dataset = BIG_QUERY_CLIENT.create_dataset(dataset, exists_ok=True)
            print(f"Dataset {DATASET_ID} created.")
        
        # Create table with sample data
        query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
        OPTIONS(description="Raw data from analyst team to ingest into Cloud Dataprep") 
        AS SELECT * FROM `data-to-insights.ecommerce.all_sessions_raw` 
        WHERE date = '20170801';  # limiting to one day of data (56k rows for this lab)
        """
        
        query_job = BIG_QUERY_CLIENT.query(query)
        query_job.result()  # Wait for query to complete
        
        print(f"Table {TABLE_ID} created with sample data.")
        return True
    except Exception as e:
        print(f"Error creating BigQuery dataset: {e}")
        return False

def connect_bigquery_to_dataprep(auth_token, flow_name="Ecommerce Analysis"):
    """Step 3: Connect BigQuery data to Cloud Dataprep - implements Task 3 of the lab."""
    proxies = get_proxies()
    
    # 1. Create a flow
    flow_id = create_flow(auth_token, flow_name, "Flow for analyzing ecommerce data", proxies)
    if not flow_id:
        return None
    
    # 2. Create a connection to BigQuery
    connection_id = create_bigquery_connection(auth_token, proxies)
    if not connection_id:
        return None
    
    # 3. Import the dataset from BigQuery
    dataset_id = import_bigquery_dataset(auth_token, flow_id, connection_id, PROJECT_ID, DATASET_ID, TABLE_ID, proxies)
    if not dataset_id:
        return None
    
    # 4. Create a wrangled dataset (recipe)
    wrangled_dataset_id = create_wrangled_dataset(auth_token, flow_id, dataset_id, f"{TABLE_ID}_transformed", proxies)
    
    return {
        "flow_id": flow_id,
        "connection_id": connection_id,
        "dataset_id": dataset_id,
        "wrangled_dataset_id": wrangled_dataset_id
    }

def create_flow(auth_token, flow_name, flow_description, proxies=None):
    """Create a new flow in Cloud Dataprep."""
    url = "https://api.clouddataprep.com/v4/flows"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": flow_name,
        "description": flow_description
    }

    response = requests.post(url, headers=headers, json=payload, proxies=proxies)
    if response.status_code == 201:
        print("Flow created successfully.")
        return response.json().get("id")
    else:
        print("Failed to create flow.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return None

def create_bigquery_connection(auth_token, proxies=None):
    """Create a connection to BigQuery in Cloud Dataprep."""
    url = "https://api.clouddataprep.com/v4/connections"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": "BigQuery Connection",
        "description": "Connection to BigQuery for ecommerce data",
        "type": "rest",
        "vendor": "google",
        "vendorName": "google",
        "credentialType": "apiKey",
        "params": {
            "projectId": PROJECT_ID,
            "serviceAccountPath": "default"  # Use default service account
        }
    }

    response = requests.post(url, headers=headers, json=payload, proxies=proxies)
    if response.status_code == 201:
        print("BigQuery connection created successfully.")
        return response.json().get("id")
    else:
        print("Failed to create BigQuery connection.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return None

def import_bigquery_dataset(auth_token, flow_id, connection_id, project_id, dataset_id, table_id, proxies=None):
    """Import a dataset from BigQuery into Cloud Dataprep."""
    url = "https://api.clouddataprep.com/v4/importedDatasets"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": table_id,
        "description": f"Imported from BigQuery table {project_id}.{dataset_id}.{table_id}",
        "flow": {"id": flow_id},
        "connection": {"id": connection_id},
        "path": f"{project_id}.{dataset_id}.{table_id}"
    }

    response = requests.post(url, headers=headers, json=payload, proxies=proxies)
    if response.status_code == 201:
        print("Dataset imported from BigQuery successfully.")
        return response.json().get("id")
    else:
        print("Failed to import dataset from BigQuery.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return None

def create_wrangled_dataset(auth_token, flow_id, imported_dataset_id, name, proxies=None):
    """Create a wrangled dataset (recipe) from an imported dataset."""
    url = "https://api.clouddataprep.com/v4/wrangledDatasets"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": name,
        "vendor": "google",
        "venodrName": "google",
        "description": f"Transformed dataset for {name}",
        "flow": {"id": flow_id},
        "importedDataset": {"id": imported_dataset_id}
    }

    response = requests.post(url, headers=headers, json=payload, proxies=proxies)
    if response.status_code == 201:
        print("Wrangled dataset created successfully.")
        return response.json().get("id")
    else:
        print("Failed to create wrangled dataset.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return None

def run_dataprep_job(auth_token=None, wrangled_dataset_id=None):
    """Step 5: Run Cloud Dataprep jobs to BigQuery - implements Task 7 of the lab."""
    if not auth_token:
        auth_token = get_token()
    
    if not auth_token:
        print("Authentication token not available.")
        return None
    
    proxies = get_proxies()
    
    if not wrangled_dataset_id:
        print("No wrangled dataset ID provided. Please connect to Dataprep first.")
        return None
    
    # Run the transformation job with output to BigQuery
    url = "https://api.clouddataprep.com/v4/jobGroups"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "wrangledDataset": {"id": wrangled_dataset_id},
        "runParameters": {
            "overrides": {
                "execution": "dataflow",
                "profiler": True,
                "outputFormat": "json",
                "writesettings": [
                    {
                        "path": f"{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}",
                        "action": "create",
                        "format": "bigquery",
                        "writeDisposition": "drop",
                        "table": OUTPUT_TABLE,
                        "location": "US",
                        "createDisposition": "create-empty"
                    }
                ]
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload, proxies=proxies)
    if response.status_code == 201:
        job_id = response.json().get("id")
        print(f"Cloud Dataprep job started successfully with job ID: {job_id}")
        
        # Check job status
        job_status = check_job_status(auth_token, job_id, proxies)
        return job_id
    else:
        print("Failed to start Cloud Dataprep job.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return None

def check_job_status(auth_token, job_id, proxies=None):
    """Check the status of a Cloud Dataprep job."""
    url = f"https://api.clouddataprep.com/v4/jobGroups/{job_id}"
    headers = {
        "Authorization": f"Bearer {auth_token}"
    }

    response = requests.get(url, headers=headers, proxies=proxies)
    if response.status_code == 200:
        status = response.json().get("status")
        print(f"Job status: {status}")
        return status
    else:
        print("Failed to get job status.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return None

if __name__ == "__main__":
    main()
