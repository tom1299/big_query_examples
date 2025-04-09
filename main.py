# See https://www.cloudskillsboost.google/paths/17/course_templates/1171/documents/512680
# Chapter Architecting low-code ML solutions
# Getting Started with BigQuery ML:
# https://www.cloudskillsboost.google/course_templates/626/labs/489287

from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import random

BIG_QUERY_CLIENT = None
DATA_SET_NAME = "bqml_lab"
MODEL_NAME = f"test-model-{random.randint(100000, 999999)}"
# Depending on the nature of the prediction, either linear_reg or logistic_reg are appropriate.
# Linear regression: "How many purchases will the customer make?"
# Logistic regression: "Will the customer make a purchase?"
MODEL_TYPE = "linear_reg"

PROJECT = "intrepid-signal-310513"

def main():
    print("Welcome to the Big Query Examples project!")
    authenticate_with_gcp()

def authenticate_with_gcp():

    credentials = service_account.Credentials.from_service_account_file(
        '/home/reuhl/git/github/big_query_examples/authentication/credentials.json'
    )
    global BIG_QUERY_CLIENT
    BIG_QUERY_CLIENT = bigquery.Client(credentials=credentials)

def get_dataset(dataset_name):
    try:
        dataset = BIG_QUERY_CLIENT.get_dataset(dataset_name)
    except NotFound:
        print(f"Dataset {dataset_name} not found.")
        return None
    return dataset

def create_dataset(dataset_name):
    dataset = bigquery.Dataset(dataset_name)
    # Set default table expiration to 59 days (in seconds)
    dataset.default_table_expiration_ms = 59 * 24 * 60 * 60 * 1000  # 59 days in milliseconds
    # Set default partition expiration to 59 days (in seconds)
    dataset.default_partition_expiration_ms = 59 * 24 * 60 * 60 * 1000  # 59 days in milliseconds
    dataset = BIG_QUERY_CLIENT.create_dataset(dataset, exists_ok=True)
    print(f"Created dataset {dataset.dataset_id} with a default expiration of 59 days and partition expiration of 59 days")

def delete_dataset(dataset_name):
    BIG_QUERY_CLIENT.delete_dataset(dataset_name, delete_contents=True)
    print(f"Deleted dataset {dataset_name}")

def list_datasets():
    datasets = BIG_QUERY_CLIENT.list_datasets()
    if not datasets:
        print("No datasets found.")
        return

    page_token = None
    while True:
        response = BIG_QUERY_CLIENT.list_datasets(page_token=page_token)
        for dataset in response:
            print(f"Dataset ID: {dataset.dataset_id}")
        page_token = response.next_page_token
        if not page_token:
            break

def run_sql_query(query):
    query_job = BIG_QUERY_CLIENT.query(query)
    results = query_job.result(page_size=100)
    rows = list(results)
    if rows:
        headers = rows[0].keys()
        column_widths = {header: max(len(header), *(len(str(row[header])) for row in rows)) for header in headers}

        # Print header
        header_row = " | ".join(header.ljust(column_widths[header]) for header in headers)
        print(header_row)
        print("-" * len(header_row))

        # Print rows
        for row in rows:
            print(" | ".join(str(row[header]).ljust(column_widths[header]) for header in headers))
    else:
        print("No results found.")

if __name__ == "__main__":
    authenticate_with_gcp()
    
    dataset = get_dataset(f"{PROJECT}.{DATA_SET_NAME}")
    
    if not dataset:
        print("Dataset not found, creating a new one.")
        dataset = create_dataset(f"{PROJECT}.{DATA_SET_NAME}")
    list_datasets()

    # SQL statement to create or replace a logistic regression model in BigQuery ML
    create_ml_model_sql = f"""
    CREATE OR REPLACE MODEL `{PROJECT}.{DATA_SET_NAME}.{MODEL_NAME}`
    OPTIONS(
        model_type = '{MODEL_TYPE}'  -- Specify the type of model as logistic regression
    ) AS
    SELECT
        IF(totals.transactions IS NULL, 0, 1) AS label,  -- Binary label: 1 if transactions exist, otherwise 0
        IFNULL(device.operatingSystem, "") AS os,        -- Operating system, default to empty string if NULL
        device.isMobile AS is_mobile,                   -- Boolean indicating if the device is mobile
        IFNULL(geoNetwork.country, "") AS country,      -- Country, default to empty string if NULL
        IFNULL(totals.pageviews, 0) AS pageviews        -- Number of pageviews, default to 0 if NULL
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`  -- Public dataset for Google Analytics sample
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170631'  -- Filter data by date range
    LIMIT 3000;  -- Limit the number of rows to 3000 to avoid excessive data usage
    """
    # LIMIT may cause "Classification model requires at least 2 unique labels and the label column had only 1 unique label.".
    # See https://stackoverflow.com/questions/52821814/bigquery-logistic-regression-issue

    run_sql_query(create_ml_model_sql)
    print("Model created successfully.")

    # SQL statement to evaluate the logistic regression model using BigQuery ML
    evaluate_model_sql = f"""
    SELECT
        *  -- Select all columns from the evaluation results
    FROM
        ml.EVALUATE(
            MODEL `{PROJECT}.{DATA_SET_NAME}.{MODEL_NAME}`,  -- Specify the model to evaluate
            (
                SELECT
                    IF(totals.transactions IS NULL, 0, 1) AS label,  -- Binary label: 1 if transactions exist, otherwise 0
                    IFNULL(device.operatingSystem, "") AS os,        -- Operating system, default to empty string if NULL
                    device.isMobile AS is_mobile,                   -- Boolean indicating if the device is mobile
                    IFNULL(geoNetwork.country, "") AS country,      -- Country, default to empty string if NULL
                    IFNULL(totals.pageviews, 0) AS pageviews        -- Number of pageviews, default to 0 if NULL
                FROM
                    `bigquery-public-data.google_analytics_sample.ga_sessions_*`  -- Public dataset for Google Analytics sample
                WHERE
                    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'  -- Filter data by date range for evaluation
            )
        );
    """

    # Example output of the evaluation for the logistic regression model:
    # precision           | recall               | accuracy           | f1_score             | log_loss            | roc_auc           
    # ---------------------------------------------------------------------------------------------------------------------------------
    # 0.39622641509433965 | 0.019553072625698324 | 0.9854103915662651 | 0.037267080745341616 | 0.06257513 | 0.75
    #
    # Explanation: 
    # - precision: Proportion of true positive predictions out of all positive predictions.
    # - recall: Proportion of true positives identified out of all actual positives.
    # - accuracy: Proportion of correct predictions (both true positives and true negatives) out of all predictions.
    # - f1_score: Harmonic mean of precision and recall, balancing the two metrics.
    # - log_loss: Logarithmic loss, measuring the model's prediction uncertainty.
    # - roc_auc: Area under the ROC curve, indicating the model's ability to distinguish between classes (higher is better).

    # Example output of the evaluation for the linear regression model:
    # mean_absolute_error  | mean_squared_error   | mean_squared_log_error | median_absolute_error | r2_score            | explained_variance
    # ---------------------------------------------------------------------------------------------------------------------------------------
    # 0.032263135454324646 | 0.013179852067544389 | 0.006501012583225432   | 0.010068682901201331  | 0.07400203007353712 | 0.0749363400277151
    #
    # Explanation:
    # - mean_absolute_error: Average absolute difference between predicted and actual values. Lower is better.
    # - mean_squared_error: Average squared difference between predicted and actual values. Lower is better.
    # - mean_squared_log_error: Measures the ratio of predicted to actual values. Lower is better.
    # - median_absolute_error: Median of absolute differences between predicted and actual values. Lower is better.
    # - r2_score: Proportion of variance in the dependent variable explained by the model. Higher is better (ranges from 0 to 1).
    # - explained_variance: Similar to r2_score, measures how well the model explains the data. Higher is better.
    #
    # In this case, the r2_score and explained_variance are relatively low, indicating the model may not be very effective

    run_sql_query(evaluate_model_sql)
    print("Model evaluated successfully.")

    # SQL statement for predicting the number of purchases for individual users:
    predict_model_sql = f"""
    SELECT
        fullVisitorId,  -- Unique identifier for each visitor
        SUM(predicted_label) AS total_predicted_purchases  -- Sum of predicted labels (purchases) for each visitor
    FROM
        ml.PREDICT(
            MODEL `{PROJECT}.{DATA_SET_NAME}.{MODEL_NAME}`,  -- Specify the model to use for predictions
            (
                SELECT
                    IFNULL(device.operatingSystem, "") AS os,  -- Operating system, default to empty string if NULL
                    device.isMobile AS is_mobile,             -- Boolean indicating if the device is mobile
                    IFNULL(totals.pageviews, 0) AS pageviews, -- Number of pageviews, default to 0 if NULL
                    IFNULL(geoNetwork.country, "") AS country, -- Country, default to empty string if NULL
                    fullVisitorId                             -- Unique identifier for each visitor
                FROM
                    `bigquery-public-data.google_analytics_sample.ga_sessions_*`  -- Public dataset for Google Analytics sample
                WHERE
                    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'  -- Filter data by date range for prediction
            )
        )
    GROUP BY
        fullVisitorId  -- Group results by visitor ID
    ORDER BY
        total_predicted_purchases DESC  -- Order results by the total predicted purchases in descending order
    LIMIT 10;  -- Limit the results to the top 10 visitors with the highest predicted purchases
    """
    run_sql_query(predict_model_sql)
    print("Predictions made successfully.")
    # Example output of the prediction:
    # fullVisitorId       | total_predicted_purchases
    # -----------------------------------------------
    # 0691459609787345904 | 2.8497667534207265       
    # 9417857471295131045 | 2.45502655791311         
    # 7090844536719687007 | 1.893965646106018        
    # 8388931032955052746 | 1.8209734782554703       
    # 7798080316988640454 | 1.7268645409857755 
   
    delete_model_sql = f"DROP MODEL IF EXISTS `{PROJECT}.{DATA_SET_NAME}.{MODEL_NAME}`;"
    run_sql_query(delete_model_sql)