# Task 1

## Steps
1. Download and install docker and docker compose
2. Run `docker-compose up --build -d`. It will deploy Apache Airflow, PostgreSQL and MySQL
3. Connect to MySQL (localhost:3307) and restore db from test's question
4. Set Airflow Variable mysql_db_url = `mysql+pymysql://root:1234@localhost:3307/hr`
5. Create hr table on PostgreSQL
6. Set Airflow Variable postgres_db_url = `postgresql+psycopg2://airflow:airflow@localhost:5433/hr`
7. Run DAG Airflow `etl_mysql_to_postgres`