from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import json


def get_timestamp(date_string):
    date_format = "%Y%m%dT%H%M%S"
    datetime_object = datetime.strptime(date_string, date_format)
    return int(datetime.timestamp(datetime_object))


def _process_weather(**kwargs):
    info = kwargs["ti"].xcom_pull(kwargs["extract_data_task_id"])
    timestamp = info["current"]["dt"]
    temp = info["current"]["temp"]
    humidity = info["current"]["humidity"]
    clouds = info["current"]["clouds"]
    wind_speed = info["current"]["wind_speed"]
    return kwargs["city"], timestamp, temp, humidity, clouds, wind_speed


with DAG(dag_id="weather_hw1",
         schedule_interval="@daily",
         start_date=datetime(2023, 11, 27),
         catchup=True,
         user_defined_macros={
             'get_historic_dt': get_timestamp
         }
         ) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="weather_pg_conn",
        sql="sql/measures.sql",
    )

    data = {
        "Lviv": {"lat": "49.84195", "lon": "24.031592"},
        "Kyiv": {"lat": "50.4500336", "lon": "30.5241361"},
        "Kharkiv": {"lat": "49.9923181", "lon": "36.2310146"},
        "Odesa": {"lat": "46.4843023", "lon": "30.7322878"},
        "Zhmerynka": {"lat": "49.0354593", "lon": "28.1147317"}
    }

    for city, coordinates in data.items():
        req = {"appid": Variable.get("WEATHER_API_KEY"),
               "lat": coordinates["lat"],
               "lon": coordinates["lon"],
               "dt": "{{ get_historic_dt(ts_nodash) }}"}

        check_api = HttpSensor(
            task_id=f"check_api_{city}",
            http_conn_id="weather_http_conn",
            endpoint="data/3.0/onecall",
            request_params=req)

        get_data = SimpleHttpOperator(
            task_id=f"get_data_{city}",
            http_conn_id="weather_http_conn",
            endpoint="data/3.0/onecall",
            data=req,
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
        )

        process_data = PythonOperator(
            task_id=f"process_data_{city}",
            python_callable=_process_weather,
            provide_context=True,
            op_kwargs={"city": city, "extract_data_task_id": f"extract_data_{city}"}
        )

        push_to_table = PostgresOperator(
            task_id=f"push_to_table_{city}",
            postgres_conn_id="weather_pg_conn",
            sql="sql/fill.sql",
            parameters=[city,
                        "{{ ti.xcom_pull(task_ids='process_data_" + city + ")[0] }}",
                        "{{ ti.xcom_pull(task_ids='process_data_" + city + ")[1] }}",
                        "{{ ti.xcom_pull(task_ids='process_data_" + city + ")[2] }}",
                        "{{ ti.xcom_pull(task_ids='process_data_" + city + ")[3] }}",
                        "{{ ti.xcom_pull(task_ids='process_data_" + city + ")[4] }}"]
        )

        create_table >> check_api >> get_data >> process_data >> push_to_table
