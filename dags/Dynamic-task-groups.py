from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
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
    start = EmptyOperator(
        task_id = 'start'
    )

    end = EmptyOperator(
        task_id = 'end'
    )
    groups=[]
    for g_id in range (1,4):
        with TaskGroup(group_id=f'group{g_id}') as tg1:
            t1 = EmptyOperator(
                task_id = 'start'
            )
            t2 = EmptyOperator(
                task_id = 'end'
            )
            sub_groups=[]
            for s_id in range (1,4):
                with TaskGroup(group_id=f'sub_group{s_id}') as tg2:
                    st1 = EmptyOperator(
                        task_id = 'start'
                    )
                    st2= EmptyOperator(
                        task_id = 'end'
                    )
                    print_date_2 = BashOperator(
                        task_id = 'print_date',
                        bash_command='date'
                    )
                    st1 >> print_date_2 >> st2
                    sub_groups.append(tg2)

            print_date_1 = BashOperator(
                task_id='print_date',
                bash_command = 'date'
            )
    
            t1 >> [sub_groups[0], sub_groups[1]] >> sub_groups[2] >> print_date_1 >> t2
            groups.append(tg1)

    start >> [groups[0],groups[1]] >> groups[2] >> end

dag = taskflow()
