from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator

from dags.prepare import define_prepare
from dags.import_readings import define_import_readings
from dags.bus_speed import define_bus_speed

from datetime import datetime


default_args = {
        'depends_on_past': False,
        'start_date': datetime(2015, 1, 1),
        'end_date': datetime(2016, 3, 31),
        'email': ['thcsquad@gmail.com']
}

MAIN_DAG_NAME = 'better_transit'

dag = DAG(
        dag_id=MAIN_DAG_NAME,
        schedule_interval='@daily',
        default_args=default_args
)

prepare = SubDagOperator(
    subdag=define_prepare(MAIN_DAG_NAME),
    task_id='prepare',
    dag=dag
)

import_readings = SubDagOperator(
    subdag=define_import_readings(MAIN_DAG_NAME),
    task_id='import_readings',
    dag=dag
)

bus_speeds = SubDagOperator(
    subdag=define_bus_speed(MAIN_DAG_NAME),
    task_id='bus_speed',
    dag=dag
)

import_readings.set_upstream(prepare)
bus_speeds.set_upstream(import_readings)
