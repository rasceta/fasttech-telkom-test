from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from operators.mysql_to_postgres import MySQLToPostgresOperator


default_args = {
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

SQL_COUNTRIES = """SELECT * FROM countries"""
SQL_DEPARTMENTS = """SELECT * FROM departments"""
SQL_EMPLOYEES = """SELECT * FROM employees"""
SQL_JOB_HISTORY = """SELECT * FROM job_history"""
SQL_JOBS = """SELECT * FROM jobs"""
SQL_LOCATIONS = """SELECT * FROM locations"""
SQL_REGIONS = """SELECT * FROM regions"""

with DAG(
    'etl_mysql_to_postgres',
    default_args=default_args,
    description='ETL jobs MySQL to PostgreSQL',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False
) as dag:

    start_task = DummyOperator(task_id='start_task')

    t1 = MySQLToPostgresOperator(
        task_id="etl_countries_to_postgres",
        sql = SQL_COUNTRIES,
        mysql_db_url=Variable.get("mysql_db_url"),
        postgres_db_url=Variable.get("postgres_db_url"),
        postgres_table="countries"
    )

    t2 = MySQLToPostgresOperator(
        task_id="etl_departments_to_postgres",
        sql = SQL_DEPARTMENTS,
        mysql_db_url=Variable.get("mysql_db_url"),
        postgres_db_url=Variable.get("postgres_db_url"),
        postgres_table="departments"
    )

    t3 = MySQLToPostgresOperator(
        task_id="etl_employees_to_postgres",
        sql = SQL_EMPLOYEES,
        mysql_db_url=Variable.get("mysql_db_url"),
        postgres_db_url=Variable.get("postgres_db_url"),
        postgres_table="employees"
    )

    t4 = MySQLToPostgresOperator(
        task_id="etl_job_history_to_postgres",
        sql = SQL_JOB_HISTORY,
        mysql_db_url=Variable.get("mysql_db_url"),
        postgres_db_url=Variable.get("postgres_db_url"),
        postgres_table="job_history"
    )

    t5 = MySQLToPostgresOperator(
        task_id="etl_jobs_to_postgres",
        sql = SQL_JOBS,
        mysql_db_url=Variable.get("mysql_db_url"),
        postgres_db_url=Variable.get("postgres_db_url"),
        postgres_table="jobs"
    )

    t6 = MySQLToPostgresOperator(
        task_id="etl_locations_to_postgres",
        sql = SQL_LOCATIONS,
        mysql_db_url=Variable.get("mysql_db_url"),
        postgres_db_url=Variable.get("postgres_db_url"),
        postgres_table="locations"
    )

    t7 = MySQLToPostgresOperator(
        task_id="etl_regions_to_postgres",
        sql = SQL_REGIONS,
        mysql_db_url=Variable.get("mysql_db_url"),
        postgres_db_url=Variable.get("postgres_db_url"),
        postgres_table="regions"
    )

    end_task = DummyOperator(task_id='end_task')

    start_task >> [t1,t2,t3,t4,t5,t6,t7] >> end_task