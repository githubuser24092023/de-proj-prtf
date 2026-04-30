from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime

with DAG(
    dag_id='init_db',
    start_date=datetime(year=2026, month=4, day=1),
    schedule=None,
    tags=['init', 'DB'],
    catchup=False
) as dag:
    create_CrpCcy_schema = SQLExecuteQueryOperator(
        task_id='create_CrpCcy_schema',
        conn_id='pg_conn_default',
        sql="create schema if not exists CrpCcy;"
    )

    create_OKX_rates_table = SQLExecuteQueryOperator(
        task_id='create_OKX_rates_table',
        conn_id='pg_conn_default',
        sql="""
            create table if not exists CrpCcy.OKX_rates
            (
              inst_id varchar(35) not null,
              price numeric(20, 10) not null,
              gotten_at timestamp not null default current_timestamp,
              primary key(inst_id, gotten_at)
            );
        """
    )

    create_FNG_statuses_table = SQLExecuteQueryOperator(
        task_id='create_FNG_statuses_table',
        conn_id='pg_conn_default',
        sql="""
            create table if not exists CrpCcy.FNG_rates
            (
              rate_value int not null,
              rate_state varchar(25) not null,
              gotten_at date primary key default current_date
            );
        """
    )

    create_CrpCcy_schema >> [create_OKX_rates_table, create_FNG_statuses_table]