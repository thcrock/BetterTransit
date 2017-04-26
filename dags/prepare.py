from airflow import DAG
from airflow.operators import BaseOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 1, 1),
    'email': ['thcsquad@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def define_prepare(metadag_name):
    dag = DAG(
        '{}.prepare'.format(metadag_name),
        default_args=default_args,
        schedule_interval='@once'
    )

    class Partition(BaseOperator):
        def execute(self, context):
            pass

    class SetupDatabase(BaseOperator):
        def execute(self, context):
            pass

    Partition(task_id='partition', dag=dag)
    SetupDatabase(task_id='setup_db', dag=dag)

    return dag
