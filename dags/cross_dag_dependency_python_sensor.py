import requests
from datetime import datetime
import os
from airflow.decorators import dag
from airflow.sensors.python import PythonSensor


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def check_parent_dagrun_status(execution_date):
    #List DagRuns
    URL = "http://host.docker.internal:8080/api/v1/dags/sleep-10mins-dag/dagRuns"
    headers = {'Authorization':'Basic YWRtaW46YWRtaW4='}
    r = requests.get(url = URL, headers = headers)
    data = r.json()
    #Get Parent DAG execution datetime and state
    parent_dag_latest_execution_datetime = data['dag_runs'][-1]['execution_date']
    parent_dag_state = data['dag_runs'][-1]['state']
    #Extract Date from datetime
    parent_datetime_obj = datetime.strptime(parent_dag_latest_execution_datetime, '%Y-%m-%dT%H:%M:%S.%f+00:00')
    parent_dag_latest_execution_date = parent_datetime_obj.date()
    #Get current DAG execution datetime from DagRun context
    child_dag_latest_execution_datetime = execution_date
    #Extract Date from datetime
    child_datetime_obj = datetime.strptime(child_dag_latest_execution_datetime, '%Y-%m-%dT%H:%M:%S.%f+00:00')
    child_dag_latest_execution_date = child_datetime_obj.date()
    #Compare Date of child_dag with parent_dag and check state of parent dag latest dag_run
    if (parent_dag_latest_execution_date==child_dag_latest_execution_date and parent_dag_state=='success'):
        print("parent_dag_executed_successfully")
        return True
    else:
        print("parent dag execution is still in progress")
        return False

@dag(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    default_args=DEFAULT_ARGS
    )
def taskflow():

    check_upstream_task = PythonSensor(
    task_id='check_upstream_task',
    python_callable=check_parent_dagrun_status,
    op_kwargs={'execution_date':'{{ execution_date }}'},
    poke_interval=120,
    timeout=60*15,
    mode="reschedule"
    )

    check_upstream_task
    
dag = taskflow()
