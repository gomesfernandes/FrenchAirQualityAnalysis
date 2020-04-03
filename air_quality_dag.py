"""
Airflow DAG that will:
    1 - fetch the datasets from the day before
    2 - create the corresponding CSV files in a GCP bucket
    3 - launch a Dataflow job, which writes each row to BigQuery
    4 - delete the CSV files
"""
from datetime import datetime
from datetime import timedelta
import logging
import os

from airflow import configuration, DAG, models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from dateutil.parser import isoparse

import e2_parser as e2

LOGGER = logging.getLogger("airqualityflow")
GCP_PROJECT_ID = models.Variable.get('GCP_PROJECT_ID')
AIRQUALITY_BUCKET = models.Variable.get('AIRQUALITY_BUCKET')
DEFAULT_DAG_ARGS = {
    'start_date': datetime(2020, 4, 1, 12, 0),
    'project_id': GCP_PROJECT_ID,
    'schedule_interval': '@daily'
}

DATAFLOW_ARGS = {
    'input': AIRQUALITY_BUCKET,
    'output': models.Variable.get('E2_TABLE'),
    'project': GCP_PROJECT_ID,
    'job_name': 'airqualityflow',
    'region': 'us-central1',
    'save_main_session': '',
    'staging_location': '{}/temp/'.format(AIRQUALITY_BUCKET),
    'temp_location': '{}/temp'.format(AIRQUALITY_BUCKET),
    'runner': 'DataflowRunner'
}

DATAFLOW_FILE = os.path.join(configuration.get('core', 'dags_folder'), 'air_quality_flow.py')


def create_csv_files(bucket, **context):
    LOGGER.info('create_csv_files input date : ' + str(context['execution_date']))

    search_date = isoparse(str(context['execution_date'])) - timedelta(days=1)
    resources_list = e2.get_resources_for_date(search_date)
    LOGGER.info('Found the following resources : ' + str(resources_list))
    new_files = []
    for resource in resources_list:
        try:
            rows, new_filename = e2.format_resource_to_csv(resource)
        except ValueError:
            continue
        e2.write_to_bucket(rows, new_filename, bucket.replace('gs://', ''), with_header=False)
        new_files.append(os.path.join(bucket, new_filename))
    task_instance = context['task_instance']
    task_instance.xcom_push(key='files_to_delete', value=' '.join(new_files))


with DAG(
        dag_id='air_quality_dag',
        description='Daily Air Quality dataset import',
        max_active_runs=1,
        # catchup=True, //TODO : allow parallelization of DAG runs (currently max_active_runs = 1)
        default_args=DEFAULT_DAG_ARGS) as dag:

    fetch_task = PythonOperator(
        task_id='get_latest_datasets',
        python_callable=create_csv_files,
        op_args=[AIRQUALITY_BUCKET],
        provide_context=True,
        dag=dag
    )

    dataflow_task = DataFlowPythonOperator(
        task_id='push_to_bigquery',
        py_file=DATAFLOW_FILE,
        options=DATAFLOW_ARGS,
        retries=2,
        retry_delay=timedelta(minutes=5),
        dag=dag
    )

    delete_csv_task = BashOperator(
        task_id='delete_csv_files',
        bash_command="gsutil rm {{ task_instance.xcom_pull(task_ids='get_latest_datasets', key='files_to_delete') }}",
        dag=dag
    )

    fetch_task >> dataflow_task >> delete_csv_task
