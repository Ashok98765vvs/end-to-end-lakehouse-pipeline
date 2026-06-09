"""
Airflow DAG: ingestion_dag.py
Author: Ashok Chowdary (Ashok98765vvs)
Description:
    Daily data ingestion DAG that pulls data from REST APIs and
    writes to Azure Data Lake Storage Gen2 in Delta Lake format (Bronze Layer).
    Part of the End-to-End Lakehouse Pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago
import logging
import requests
import json
import os

# ─────────────────────────────────────────
# DEFAULT ARGUMENTS
# ─────────────────────────────────────────
default_args = {
    'owner': 'ashok_chowdary',
    'depends_on_past': False,
    'email': ['ashok.shankar7156@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ─────────────────────────────────────────
# API CONFIGURATION
# ─────────────────────────────────────────
API_CONFIGS = [
    {
        'name': 'sales_api',
        'url': os.getenv('SALES_API_URL', 'https://api.example.com/v1/sales'),
        'headers': {'Authorization': f"Bearer {os.getenv('SALES_API_KEY', '')}"},
        'target_path': 'bronze/sales/',
    },
    {
        'name': 'customers_api',
        'url': os.getenv('CUSTOMERS_API_URL', 'https://api.example.com/v1/customers'),
        'headers': {'Authorization': f"Bearer {os.getenv('CUSTOMERS_API_KEY', '')}"},
        'target_path': 'bronze/customers/',
    },
    {
        'name': 'products_api',
        'url': os.getenv('PRODUCTS_API_URL', 'https://api.example.com/v1/products'),
        'headers': {'Authorization': f"Bearer {os.getenv('PRODUCTS_API_KEY', '')}"},
        'target_path': 'bronze/products/',
    },
]


# ─────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────
def validate_api_connections(**context):
    """Pre-flight check: validate all API endpoints are reachable."""
    logger = logging.getLogger(__name__)
    failed = []
    for cfg in API_CONFIGS:
        try:
            response = requests.head(cfg['url'], headers=cfg['headers'], timeout=10)
            if response.status_code not in [200, 405]:
                failed.append(cfg['name'])
                logger.warning(f"API check failed for {cfg['name']}: {response.status_code}")
            else:
                logger.info(f"API check passed for {cfg['name']}")
        except Exception as e:
            failed.append(cfg['name'])
            logger.error(f"Cannot reach {cfg['name']}: {str(e)}")
    if failed:
        raise ConnectionError(f"API connectivity check failed for: {failed}")
    logger.info("All API connections validated successfully.")


def ingest_api_data(api_config: dict, execution_date: str, **context):
    """Fetch data from a REST API and write to ADLS Gen2 in JSON format."""
    logger = logging.getLogger(__name__)
    api_name = api_config['name']
    url = api_config['url']
    headers = api_config['headers']
    target_path = api_config['target_path']

    logger.info(f"Starting ingestion for: {api_name}")

    # Paginated data fetch
    all_records = []
    page = 1
    while True:
        params = {'date': execution_date, 'page': page, 'page_size': 1000}
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('data', data if isinstance(data, list) else [])
        if not records:
            break
        all_records.extend(records)
        logger.info(f"  Fetched page {page} — {len(records)} records")
        if not data.get('has_next_page', False):
            break
        page += 1

    logger.info(f"Total records fetched for {api_name}: {len(all_records)}")

    # Write to ADLS Gen2
    wasb_hook = WasbHook(wasb_conn_id='azure_data_lake')
    blob_name = f"{target_path}{execution_date}/data.json"
    blob_data = json.dumps(all_records, indent=2, default=str)
    wasb_hook.load_string(
        string_data=blob_data,
        container_name='lakehouse',
        blob_name=blob_name,
        overwrite=True,
    )
    logger.info(f"Successfully written {len(all_records)} records to: {blob_name}")

    # Push metrics to XCom for downstream quality checks
    context['ti'].xcom_push(key=f'{api_name}_record_count', value=len(all_records))
    return len(all_records)


def validate_ingestion_counts(**context):
    """Post-ingestion check: validate minimum record counts per source."""
    logger = logging.getLogger(__name__)
    ti = context['ti']
    min_required = int(os.getenv('MIN_DAILY_RECORDS', 50000))
    failures = []

    for cfg in API_CONFIGS:
        count = ti.xcom_pull(key=f"{cfg['name']}_record_count") or 0
        if count < 1000:  # Minimum per source
            failures.append(f"{cfg['name']}: {count} records (min: 1000)")
            logger.warning(f"Low record count for {cfg['name']}: {count}")
        else:
            logger.info(f"Record count OK for {cfg['name']}: {count}")

    total = sum([ti.xcom_pull(key=f"{cfg['name']}_record_count") or 0 for cfg in API_CONFIGS])
    logger.info(f"Total ingested records today: {total}")

    if failures:
        raise ValueError(f"Ingestion validation failed:\n" + "\n".join(failures))
    logger.info("All ingestion counts validated.")


# ─────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────
with DAG(
    dag_id='ingestion_dag',
    default_args=default_args,
    description='Daily data ingestion from REST APIs to ADLS Gen2 Bronze Layer',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'bronze', 'azure', 'delta-lake', 'production'],
) as dag:

    # Task 1: Pre-flight API connectivity check
    t_validate_connections = PythonOperator(
        task_id='validate_api_connections',
        python_callable=validate_api_connections,
        provide_context=True,
    )

    # Task 2-4: Parallel ingestion from 3 APIs
    ingestion_tasks = []
    for config in API_CONFIGS:
        task = PythonOperator(
            task_id=f"ingest_{config['name']}",
            python_callable=ingest_api_data,
            op_kwargs={
                'api_config': config,
                'execution_date': '{{ ds }}',
            },
            provide_context=True,
        )
        ingestion_tasks.append(task)

    # Task 5: Post-ingestion record count validation
    t_validate_counts = PythonOperator(
        task_id='validate_ingestion_counts',
        python_callable=validate_ingestion_counts,
        provide_context=True,
    )

    # Task 6: Trigger downstream transformation DAG
    t_trigger_transform = BashOperator(
        task_id='trigger_transformation_dag',
        bash_command='airflow dags trigger transformation_dag --exec-date {{ ds }}',
    )

    # ─── DAG DEPENDENCIES ───
    # validate_connections >> [ingest_sales, ingest_customers, ingest_products] >> validate_counts >> trigger_transform
    t_validate_connections >> ingestion_tasks >> t_validate_counts >> t_trigger_transform
