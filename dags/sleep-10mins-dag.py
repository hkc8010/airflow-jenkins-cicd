from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from include.operators.customBashOperator import CustomBashOperator
import os
default_args = {
    'owner': 'Hemkumar',
    'depends_on_past': False,
    'start_date': datetime(2020,7,21),
    'email': ['hemkumar.chheda@astronomer.io'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
    }
@dag(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    schedule_interval=None,
    max_active_runs=1,
    default_args=default_args
    )
def taskflow():
    task_count=[1,2,3,4,5,6,7,8,9,10]
    t1= EmptyOperator(task_id='start')
    for i in task_count:
        t2=CustomBashOperator(
            task_id='sleep-10mins-%s' %i,
            bash_command="sleep 4m",
            retries=1,
            )
    t1 >> t2
dag = taskflow()