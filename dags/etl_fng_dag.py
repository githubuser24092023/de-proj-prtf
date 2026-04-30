from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import requests
from requests.exceptions import HTTPError, Timeout, RequestException, JSONDecodeError
from psycopg2 import ProgrammingError, OperationalError


def fetch_FNG_data():
    """
    The function gets 'Fear and Greed Index' data from alternative.me via API and returns the latest available Index
    """
    url = f"https://api.alternative.me/fng/?limit=1"
    try:
        response = requests.get(url, timeout=(5, 10))
        response.raise_for_status()

        answer = response.json()
        data = answer.get('data')[0]
        if 'time_until_update' in data.keys():
            data.pop('time_until_update')
        return data

    except Timeout:
        print("ERROR while fetching data via API: could not connect the FNG service in given timeout")
    except HTTPError as httperr:
        code = httperr.response.status_code
        if code >= 400 and code < 500:
            print("ERROR while fetching data via API: the URL used to connect FNG is invalid")
        if code >= 500:
            print("ERROR while fetching data via API: the FNG service is unavailable")
    except JSONDecodeError:
        print("ERROR while fetching data via API: fetched from FNG data have unexpected format")
    except IndexError:
        print("ERROR while fetching data via API: fetched from FNG data are empty")
    except RequestException:
        print("ERROR while fetching data via API [FNG]: unknown mistake")


def save_FNG_data(ti):
    try:
        rate = ti.xcom_pull(task_ids='fetch_FNG_data')
        if not rate:
            raise ValueError
        
        pg = PostgresHook(postgres_conn_id='pg_conn_default')
        sql = "insert into CrpCcy.FNG_rates(rate_value, rate_state, gotten_at) values (%s, %s, %s);"

        parameters = (rate['value'], rate['value_classification'], datetime.fromtimestamp(float(rate['timestamp'])))
        pg.run(sql, parameters=parameters)

    except ValueError:
        print("ERROR while saving data to DB: no data to save were extracted")
    except ProgrammingError:
        print("ERROR while saving data to DB: given request is incorrect (perhaps, a typo)")
    except OperationalError:
        print("ERROR while saving data to DB: could not connect to DB (probably, connection is invalid)")


with DAG(
    dag_id='FNG_ETL',
    start_date=datetime(year=2026, month=4, day=25),
    schedule='@daily',
    tags=['from API', 'to DB', 'FNG', 'Alternative.me'],
    catchup=False
) as dag:
    fetch_data_from_Alternativeme = PythonOperator(
        task_id='fetch_FNG_data',
        python_callable=fetch_FNG_data
    )
    save_FNG_data_to_db = PythonOperator(
        task_id='save_FNG_data',
        python_callable=save_FNG_data
    )

    fetch_data_from_Alternativeme >> save_FNG_data_to_db