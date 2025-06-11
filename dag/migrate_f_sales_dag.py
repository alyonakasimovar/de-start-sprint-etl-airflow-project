# DAG для миграции схемы и данных для staging.user_order_log.
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
        'migrate_user_order_log_schema_and_data',
        default_args=args,
        description='Миграция схемы и данных для таблицы staging.user_order_log (добавление статусов shipped/refunded)',
        catchup=False,
        schedule_interval=None,
        start_date=datetime(2025, 6, 10)
) as dag:
    # 1. Добавление колонки status в staging.user_order_log.
    add_status_column = PostgresOperator(
        task_id='update_user_order_log',
        postgres_conn_id=postgres_conn_id,
        sql="ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS status VARCHAR(20);")

    # 2. Обновление исторических данных: status = 'shipped'.
    update_historical_data = PostgresOperator(
        task_id='update_historical_data',
        postgres_conn_id=postgres_conn_id,
        sql="UPDATE staging.user_order_log SET status = 'shipped' WHERE status IS NULL;")

    add_status_column >> update_historical_data
