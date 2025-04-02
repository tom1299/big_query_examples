from google.oauth2 import service_account
from google.cloud import bigquery

def main():
    print("Welcome to the Big Query Examples project!")
    authenticate_with_gcp()

def authenticate_with_gcp():

    credentials = service_account.Credentials.from_service_account_file(
        '/home/reuhl/git/github/big_query_examples/authentication/credentials.json'
    )
    client = bigquery.Client(credentials=credentials)
    # List datasets in the project
    datasets = client.list_datasets()
    print("Datasets in project:")
    for dataset in datasets:
        print(f" - {dataset.dataset_id}")
    return client

if __name__ == "__main__":
    main()
