from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
        dag_id="clean",
        schedule_interval="@daily",
        start_date=datetime(2023, 11, 27),
        catchup=True,
) as dag:
    create_table = PostgresOperator(
        task_id="delete_table",
        postgres_conn_id="weather_pg_conn",
        sql="""
        DROP TABLE measures;
        """,
    )
