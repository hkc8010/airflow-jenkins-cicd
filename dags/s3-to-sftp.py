from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
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

    t1 = EmptyOperator(task_id='start')
    t4 = EmptyOperator(task_id='end')
    t2 = S3ToSFTPOperator(
        task_id='move_files_from_s3_to_sftp',
        sftp_conn_id='sftp_default',
        s3_conn_id='s3_default',
        s3_bucket="hem-test-airflow",
        sftp_path='/home/ec2-user/xcom.csv',
        s3_key='2022-07-15/xcom.csv'
    )
    t3 = SSHOperator(
        task_id='list_files_on_remote_server',
        ssh_conn_id='sftp_default',
        command="ls -ltrh /home/ec2-user"
    )
    t1 >> t2 >> t3 >> t4
dag = taskflow()