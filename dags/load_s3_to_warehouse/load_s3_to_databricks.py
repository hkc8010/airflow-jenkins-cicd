from airflow import DAG, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow_common.plugins.operators.databricks_custom import DatabricksSQLOperator
from airflow_common.plugins.operators.s3_list_custom import S3ListCustomOperator
from airflow_common.plugins.operators.databricks_custom import DatabricksCopyInto
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow_common.dags.load_s3_to_warehouse.config_objects.s3_config import S3Config
from airflow_common.dags.load_s3_to_warehouse.config_objects.databricks_config import DatabricksConfig
# from datahub_provider.entities import Dataset
from airflow_common.utils.alerts.slack_alert import task_fail_slack_alert

import re
import os
import ast


class LoadS3ToDatabricks:
    """
    Class to add tasks and dependencies to the dag for loading the data from S3 to databricks.

    :param dag: The dag object to which new tasks are added
    :type dag: DAG
    :param data_source: Data source type
    :type data_source: str
    :param s3_config: S3 configuration to be used for creating tasks
    :type s3_config: S3Config
    :param databricks_config: databricks configuration to be used for creating tasks
    :type databricks_config: DatabricksConfig
    :param project_directory: The root path of the project directory
    :type project_directory: str
    :param task_id_prefix: Prefix for all the task_id in a dag. Useful when dag_id is same for multiple tables
    :type task_id_prefix: str
    :param is_create_start_end_dummy_task: Create dummy start and end tasks in a dag. Useful when dag_id is same for multiple tables
    :type is_create_start_end_dummy_task: bool
    :param is_s3_list_once_per_dag: Run list S3 task once and filter the files for each table in downstream. 
        Useful when dag_id is same for multiple tables and want to filter on s3 files only once
    :type is_create_start_end_dummy_task: bool
    :param is_slack_success_alerts: Wheather to send alerts on successful file processing. 
    :type is_slack_success_alerts: bool
    :param is_send_slack_failure_alerts: Wheather to send alerts on failed file processing. 
    :type is_send_slack_failure_alerts: bool
    :param slack_conn_id_success: Slack conn id for sending success alerts
    :type slack_conn_id_success: str
    :param slack_conn_id_failure: Slack conn id for sending failure alerts
    :type slack_conn_id_failure: str
    :param task_args: A dictionary of task level override args
    :type task_args: dict
    """

    def __init__(
        self, dag: DAG, data_source: str, s3_config: S3Config, target_config: DatabricksConfig, project_directory: str,
        task_id_prefix: str = '', is_create_start_end_dummy_task: bool = False, is_s3_list_once_per_dag: bool = False,
        is_send_slack_success_alerts: bool = False, is_send_slack_failure_alerts: bool = False, 
        slack_conn_id_success: str = 'slack-alerts-success', slack_conn_id_failure: str = 'slack-alerts-failure',
        task_args: dict = {},
        **kwargs):

        self.dag = dag
        self.data_source = data_source
        self.s3_config = s3_config
        self.databricks_config = target_config
        self.project_directory = project_directory
        self.task_id_prefix = task_id_prefix
        self.is_create_start_end_dummy_task = is_create_start_end_dummy_task
        self.is_s3_list_once_per_dag = is_s3_list_once_per_dag
        self.is_send_slack_success_alerts = is_send_slack_success_alerts
        self.is_send_slack_failure_alerts = is_send_slack_failure_alerts
        self.slack_conn_id_success = slack_conn_id_success
        self.slack_conn_id_failure = slack_conn_id_failure
        self.task_args = task_args

        self.skip_staging = True
        if any([self.databricks_config.upsert_id_columns, 
            self.databricks_config.dedupe_order_by_columns,
            self.databricks_config.partition_column,
            self.databricks_config.insert_columns]):
            self.skip_staging = False   

        self.copy_options = [v.format(jsonpaths_file=f's3://{self.s3_config.jsonpaths_bucket}/{self.s3_config.jsonpaths_prefix}') 
                            for v in self.databricks_config.copy_options]

    def add_task_dummy(self, task_id: str) -> task:
        return DummyOperator(task_id=task_id)

    def add_task_s3_get_files(self) -> task:
        task_id = 's3_get_files'
        return S3ListCustomOperator(
            task_id=task_id if self.is_s3_list_once_per_dag else f'{self.task_id_prefix}{task_id}',
            bucket=self.s3_config.bucket,
            prefixes=self.s3_config.prefixes,
            search_expr=self.s3_config.search_expr,
            from_date="{{ execution_date }}",
            to_date="{{ next_execution_date }}", 
            skip_run_delay_iso_duration=self.s3_config.skip_run_delay_iso_duration,       
            aws_conn_id=self.s3_config.aws_conn_id,
            wait_for_downstream=True,
            **self.task_args.get(task_id, {}))

    def add_task_check_files(self, s3_get_files_xcom_loc) -> task:
        task_id = 'check_files'    
        return PythonOperator(
            task_id=f'{self.task_id_prefix}{task_id}',
            provide_context=True,
            python_callable=self._skip_if_no_files,
            op_kwargs={
                "s3_get_files_xcom_loc": s3_get_files_xcom_loc,
                "search_expr": self.s3_config.xcom_file_search_expr},
            **self.task_args.get(task_id, {}))

    def add_task_databricks_copy_from_s3(self, s3_keys_xcom_loc=None) -> task:
        task_id = 'databricks_copy_from_s3'
        if self.is_s3_list_once_per_dag and self.s3_config.xcom_file_search_expr.startswith('^'):
            inlets_s3 = self.s3_config.xcom_file_search_expr.split('.')[0].lstrip('^')
        else:
            inlets_s3 = f"{self.s3_config.bucket}/{self.s3_config.prefixes[0].split('{')[0]}"

        return DatabricksCopyInto(
            task_id=f'{self.task_id_prefix}{task_id}',
            schema=self.databricks_config.dest_schema,
            table=self.databricks_config.dest_table,
            s3_bucket=self.s3_config.bucket,
            s3_keys_xcom_loc=s3_keys_xcom_loc,
            drop_create_ddl_sql=self.databricks_config.etl_ddl_sql_path,
            transformed_columns=self.databricks_config.transformed_columns,
            copy_options=self.databricks_config.copy_options,
            continue_copy_on_error=self.databricks_config.continue_copy_on_error,
            databricks_sql_conn_id=self.databricks_config.databricks_sql_conn_id,
            # inlets={"datasets": [Dataset("s3", inlets_s3.replace('/', '.').rstrip('.'))]},
            # outlets={"datasets": [Dataset("databricks", f"{self.databricks_config.dest_schema}.{self.databricks_config.dest_table}")]},
            **self.task_args.get(task_id, {}))


    def add_task_slack_alert_success(self, s3_get_files_xcom_loc) -> task:
        task_id = 'slack_alert_success'
        return PythonOperator(
            task_id=f'{self.task_id_prefix}{task_id}',
            provide_context=True,            
            python_callable=self._send_slack_alert,
            op_kwargs={
                "task_id": f'{self.task_id_prefix}slack_alert_success', 
                "slack_conn_id": self.slack_conn_id_success,
                "alert_type": "success",
                "message": f"*Databricks | {self.data_source.upper()} | INFO:* Completed processing files", 
                "file_list": f"{{{{ ti.xcom_pull(task_ids='{s3_get_files_xcom_loc}', key='copy_success_file_list') }}}}"},
            **self.task_args.get(task_id, {}))

    def add_task_slack_alert_copy_failure(self, s3_get_files_xcom_loc) -> task:
        task_id = 'slack_alert_copy_failure'
        return PythonOperator(
            task_id=f'{self.task_id_prefix}{task_id}',
            provide_context=True,
            python_callable=self._send_slack_alert,
            op_kwargs={
                "task_id": f'{self.task_id_prefix}slack_alert_failure', 
                "slack_conn_id": self.slack_conn_id_failure,
                "alert_type": "copy_failure",
                "message": f"*Databricks | `{self.data_source.upper()} | Error:`* Failed to process files", 
                "file_list": f"{{{{ ti.xcom_pull(task_ids='{s3_get_files_xcom_loc}', key='copy_failed_file_list') }}}}"},
            **self.task_args.get(task_id, {}))


    @staticmethod
    def _send_slack_alert(task_id, slack_conn_id, alert_type, message, file_list, **context):
        file_list = ast.literal_eval(file_list)
        if file_list:
            if alert_type == 'copy_failure':
                task_fail_slack_alert(slack_conn_id, context)

            file_message = '\n'.join([f'`{f}` due to error: {e}' if alert_type == 'copy_failure' else f for f, e in file_list.items()])
            SlackWebhookOperator(
                task_id=f"_{task_id}",
                http_conn_id=slack_conn_id,
                message=f"{message} {file_message}").execute(dict())

    @staticmethod
    def _skip_if_no_files(s3_get_files_xcom_loc=None, search_expr=None, **context):
        file_list = {file_name: size for file_name, size in (context['task_instance'].xcom_pull(s3_get_files_xcom_loc) or {}).items()
                    if (search_expr is None or re.search(search_expr, file_name))}

        if not file_list:        
            raise AirflowSkipException

        return file_list

    def add_tasks_to_dag(self) -> DAG:
        check_files = None
        
        existing_tasks = {task.task_id: task for task in self.dag.tasks if task.task_id in ('s3_get_files')}
        s3_get_files = existing_tasks.get('s3_get_files')

        with self.dag:                
            if not s3_get_files:
                s3_get_files = self.add_task_s3_get_files()

            if self.is_s3_list_once_per_dag:
                check_files = self.add_task_check_files(s3_get_files_xcom_loc=s3_get_files.task_id)
            
            databricks_copy_from_s3 = self.add_task_databricks_copy_from_s3(s3_keys_xcom_loc=check_files.task_id if check_files else s3_get_files.task_id)

            if self.is_send_slack_success_alerts:
                databricks_copy_from_s3 >> self.add_task_slack_alert_success(databricks_copy_from_s3.task_id)

            if self.is_send_slack_failure_alerts:
                if self.databricks_config.continue_copy_on_error:
                    databricks_copy_from_s3 >> self.add_task_slack_alert_copy_failure(databricks_copy_from_s3.task_id)

            if check_files:
                s3_get_files >> check_files >> databricks_copy_from_s3
            else:
                s3_get_files >> databricks_copy_from_s3

        return self.dag
