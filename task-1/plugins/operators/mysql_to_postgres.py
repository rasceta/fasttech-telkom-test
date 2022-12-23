from sqlalchemy import create_engine
import pandas as pd
from typing import Optional, Dict, Union

from airflow.models.baseoperator import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

class MySQLToPostgresOperator(BaseOperator):
    """Selects data from a MySQL database and inserts that data into a
    PostgreSQL database. Cursors are used to minimize memory usage for large
    queries.
    """

    template_fields = ("sql", "params")
    template_ext = (".sql",)

    def __init__(
        self,
        sql: str,
        postgres_table: str = "",
        mysql_db_url = "",
        postgres_db_url = "",
        params: Optional[Dict[str, Union[str, int]]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_table = postgres_table
        self.mysql_db_url = mysql_db_url
        self.postgres_db_url = postgres_db_url
        self.params = params

    def execute(self, context):
        mysql_conn = create_engine(self.mysql_db_url)
        postgres_conn = create_engine(self.postgres_db_url)

        df = pd.read_sql(self.sql, con=mysql_conn)

        df.to_sql(name=self.postgres_table, con=postgres_conn, if_exists="replace")