# Task 2

## Steps
1. Download and install docker and docker compose
2. Run `docker-compose up --build -d`. It will deploy Apache Airflow, PostgreSQL and MySQL
3. Create folder `task2` in your Google Cloud Storage Bucket. Upload all data to that folder.
4. Create Airflow Google Cloud Connection named `gcp_fasttech` with your service account JSON.
5. Set Airflow Variable `bucket_name` with your GCS bucket name
6. Set Airflow Variable `dataset_name` with yout BigQuery dataset name
7. Run Airflow DAG `etl_to_gcs_bigquery`