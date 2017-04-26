from airflow import DAG
from airflow.operators import BaseOperator
from datetime import datetime, timedelta
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 10, 4),
    'end_date': datetime(2015, 10, 5),
    'email': ['thcsquad@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def define_import_readings(metadag_name):
    dag = DAG(
        '{}.import_readings'.format(metadag_name),
        default_args=default_args,
        schedule_interval='@daily'
    )

    class ImportOperator(BaseOperator):
        def execute(self, context):
            dt = context['execution_date']
            logging.info(dt)

    ImportOperator(task_id='import', dag=dag)

    return dag
