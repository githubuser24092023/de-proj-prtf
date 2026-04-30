from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import requests
from requests.exceptions import HTTPError, Timeout, RequestException, JSONDecodeError
from psycopg2 import ProgrammingError, OperationalError


def fetch_OKX_data(instType='SPOT', instFamily=''):
    """
    The function gets SPOT data (CrpCcy) from OKX via API and returns the latest status
    """
    stocks = []

    url = f"https://okx.com/api/v5/market/tickers?instType={instType}" + (f"&instFamily={instFamily}" if instFamily != '' else "")
    try:
        response = requests.get(url, timeout=(5, 15))
        response.raise_for_status()

        data = response.json()
        spots = data.get('data', 'n/a')

        if spots != 'n/a':
            for spot in spots:
                stocks.append((spot['instId'], spot['last'], datetime.fromtimestamp(float(spot['ts'])/1000)))

        if len(stocks) == 0:
            print("LOG: no data to save, although no mistakes were seen while fetching data via OKX API")
        return stocks

    except Timeout:
        print("ERROR while fetching data via API: could not connect the OKX service in given timeout")
    except HTTPError as httperr:
        code = httperr.response.status_code
        if code >= 400 and code < 500:
            print("ERROR while fetching data via API: the URL used to connect OKX is invalid")
        elif code >= 500:
            print("ERROR while fetching data via API: the OKX service is unavailable")
    except JSONDecodeError:
        print("ERROR while fetching data via API: fetched from OKX data have unexpected format")
    except RequestException:
        print("ERROR while fetching data via API [OKX]: unknown mistake")
    except KeyError:
        print("ERROR while parsing data: fetched from OKX data have unexpected structure")


def save_OKX_data(ti):
    try:
        spots = ti.xcom_pull(task_ids='fetch_OKX_data')
        if not spots:
            raise ValueError
        
        pg = PostgresHook(postgres_conn_id='pg_conn_default')
        sql = "insert into CrpCcy.OKX_rates(inst_id, price, gotten_at) values (%s, %s, %s);"

        for spot in spots: 
            pg.run(sql, parameters=spot)

    except ValueError:
        print("ERROR while saving data to DB: no data to save were extracted")
    except ProgrammingError:
        print("ERROR while saving data to DB: given query is incorrect (perhaps, typo)")
    except OperationalError:
        print("ERROR while saving data to DB: could not connect to DB (probably, connection is invalid)")


with DAG(
    dag_id='OKX_ETL',
    start_date=datetime(year=2026, month=4, day=25),
    schedule='@hourly',
    tags=['from API', 'to DB', 'OKX'],
    catchup=False
) as dag:
    fetch_data_from_OKX = PythonOperator(
        task_id='fetch_OKX_data',
        python_callable=fetch_OKX_data,
        op_kwargs={"instType": 'SPOT', "instFamily": ''}
    )

    save_OKX_data_to_db = PythonOperator(
        task_id='save_OKX_data',
        python_callable=save_OKX_data
    )

    fetch_data_from_OKX >> save_OKX_data_to_db