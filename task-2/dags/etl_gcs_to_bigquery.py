from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

BUCKET_NAME = Variable.get("bucket_name")
DATASET_NAME = Variable.get("dataset_name")

SQL_TOP_MONTHLY_KEYWORD = f"""
with cte1 as (
  select date(date_trunc(queryTime, month)) as month,
    searchTerms,
    count(distinct cacheId) as count,
  from `{DATASET_NAME}.flights_tickets`
  where rank = 1
  group by 1,2
  order by 1,3
)

, cte2 as (
  select month,
    searchTerms,
    count,
    row_number() over (partition by month order by count desc) as rn
  from cte1
)

select month,
  searchTerms
from cte2
where rn = 1
order by 1
"""

SQL_TOP_YEARLY_KEYWORD = f"""
with cte1 as (
  select date(date_trunc(queryTime, year)) as year,
    searchTerms,
    count(distinct cacheId) as count,
  from `{DATASET_NAME}.flights_tickets`
  where rank = 1
  group by 1,2
  order by 1,3
)

, cte2 as (
  select year,
    searchTerms,
    count,
    row_number() over (partition by year order by count desc) as rn
  from cte1
)

select year,
  searchTerms
from cte2
where rn = 1
order by 1
"""

with DAG(
    'load_gcs_to_bigquery',
    default_args=default_args,
    description='Load GCS CSV to BigQuery',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False
) as dag:

    start_task = DummyOperator(task_id='start_task')

    t1 = GCSToBigQueryOperator(
        task_id='load_flights_tickets_to_bigquery',
        google_cloud_storage_conn_id='gcp_fasttech',
        bigquery_conn_id='gcp_fasttech',
        bucket=BUCKET_NAME,
        source_objects=['task2/flights_tickets*.csv'],
        destination_project_dataset_table=f'{DATASET_NAME}.flights_tickets',
        source_format='CSV',
        compression='NONE',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        encoding='utf-8',
        field_delimiter=',',
        autodetect=True,
        time_partitioning={
            'type': 'DAY',
            'field': 'queryTime',
            'requirePartitionFilter': False
        },
    )

    t2 = BigQueryExecuteQueryOperator(task_id='load_top_keyword_monthly',
        sql=SQL_TOP_MONTHLY_KEYWORD,
        gcp_conn_id='gcp_fasttech',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table=f'{DATASET_NAME}.top_keyword_monthly',
        use_legacy_sql=False
    )

    t3 = BigQueryExecuteQueryOperator(task_id='load_top_keyword_yearly',
        sql=SQL_TOP_YEARLY_KEYWORD,
        gcp_conn_id='gcp_fasttech',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table=f'{DATASET_NAME}.top_keyword_yearly',
        use_legacy_sql=False
    )

    end_task = DummyOperator(task_id='end_task')

    start_task >> t1 >> t2 >> t3 >> end_task