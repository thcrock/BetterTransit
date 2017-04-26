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


def define_bus_speed(metadag_name):
    dag = DAG(
        '{}.bus_speed'.format(metadag_name),
        default_args=default_args,
        schedule_interval='@daily'
    )

    class BusSpeedOperator(BaseOperator):
        def execute(self, context):
            dt = context['execution_date']
            logging.info(dt)

    BusSpeedOperator(task_id='bus_speed', dag=dag)

    return dag
