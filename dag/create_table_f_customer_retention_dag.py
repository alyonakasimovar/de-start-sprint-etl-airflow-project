# DAG для создания витрины mart.f_customer_retention.
# Предназначен для однократного ручного запуска.

from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


with DAG(
        'create_table_f_customer_retention',
        default_args=args,
        description='Создание таблицы mart.f_customer_retention для метрик удержания клиентов',
        catchup=False,
        schedule_interval=None,
        start_date=datetime.now()
) as dag:
    create_f_customer_retention = PostgresOperator(
        task_id='create_table_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
                period_name VARCHAR NOT NULL,
                period_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                new_customers_count INTEGER NOT NULL,
                returning_customers_count INTEGER NOT NULL,
                refunded_customer_count INTEGER NOT NULL,
                new_customers_revenue NUMERIC(12, 2) NOT NULL,
                returning_customers_revenue NUMERIC(12, 2) NOT NULL,
                customers_refunded INTEGER NOT NULL,
                CONSTRAINT f_customer_retention_pkey PRIMARY KEY (period_id, item_id));
                """)

    create_f_customer_retention
