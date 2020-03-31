"""
Airflow DAG that will:
    1 - fetch the datasets from the day before
    2 - create the corresponding CSV files in a GCP bucket
    3 - launch a Dataflow job, which writes each row to BigQuery
    4 - delete the CSV files
"""
import logging
import os

from airflow import configuration, DAG, models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.utils.dates import days_ago

import e2_parser as e2

DEFAULT_DAG_ARGS = {
    'start_date': days_ago(1, hour=11),
    'project_id': models.Variable.get('GCP_PROJECT_ID'),
    'schedule_interval': '@daily'
}

DATAFLOW_ARGS = {
    'input': models.Variable.get('AIRQUALITY_BUCKET'),
    'output': models.Variable.get('E2_TABLE'),
    'project': models.Variable.get('GCP_PROJECT_ID'),
    'job_name': 'airqualityflow',
    'region': 'europe-west1',
    'save_main_session': '',
    'staging_location': '{}/temp/'.format(models.Variable.get('AIRQUALITY_BUCKET')),
    'temp_location': '{}/temp'.format(models.Variable.get('AIRQUALITY_BUCKET')),
    'runner': 'DataflowRunner'
}

DATAFLOW_FILE = os.path.join(configuration.get('core', 'dags_folder'), 'air_quality_flow.py')


def create_csv_files(bucket):
    logging.info("create_csv_files -- bucket : " + str(bucket))
    resources_list = e2.get_resources_from_yesterday()
    logging.info("create_csv_files -- resources_list : " + str(resources_list))
    for resource in resources_list:
        rows, new_filename = e2.format_resource_to_csv(resource)
        logging.info("create_csv_files -- new_filename : " + str(new_filename))
        e2.write_to_bucket(rows, new_filename, bucket.replace('gs://', ''), with_header=False)


with DAG(dag_id='air_quality_dag',
         description='Daily Air Quality dataset import',
         default_args=DEFAULT_DAG_ARGS) as dag:

    fetch_task = PythonOperator(
        task_id='get_latest_datasets',
        python_callable=create_csv_files,
        op_args=[models.Variable.get('AIRQUALITY_BUCKET')],
        dag=dag
    )

    dataflow_task = DataFlowPythonOperator(
        task_id='push_to_bigquery',
        py_file=DATAFLOW_FILE,
        options=DATAFLOW_ARGS,
        dag=dag
    )

    delete_csv_task = BashOperator(
        task_id='delete_csv_files',
        bash_command='gsutil rm {}/*.csv'.format(models.Variable.get('AIRQUALITY_BUCKET')),
        dag=dag
    )

    fetch_task >> dataflow_task >> delete_csv_task
