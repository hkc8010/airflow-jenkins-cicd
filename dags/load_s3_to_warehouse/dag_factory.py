from airflow import DAG
from datetime import datetime, timedelta
from functools import partial
from airflow_common.utils.alerts.pager_and_slack_alert import task_fail_page_slack_alert
from airflow_common.utils.alerts.slack_alert import task_fail_slack_alert
from airflow_common.dags.load_s3_to_warehouse.config_objects.table_source_target_config import TableSourceTargetConfig
from airflow_common.dags.load_s3_to_warehouse.dag_factory.load_s3_to_redshift import LoadS3ToRedshift
from airflow_common.dags.load_s3_to_warehouse.dag_factory.load_s3_to_databricks import LoadS3ToDatabricks
from airflow_common.dags.load_s3_to_warehouse.dag_factory.load_s3_to_redshift_using_sensor import LoadS3ToRedshiftUsingSensor
from airflow_common.dags.load_s3_to_warehouse.dag_factory.load_s3_to_databricks_using_sensor import LoadS3ToDatabricksUsingSensor


class DAGFactory:
    """
    Class that provides useful method to build an Airflow DAG
    """

    def __init__(self, dag: DAG, dag_id: str, description: str, etl_conf: TableSourceTargetConfig):
        """
        params:
            dag(DAG): the object of class DAG
            dag_id(str): the name of the dag
            description: description for DAG
            etl_conf(TableSourceTargetConfig): ETL config object of class TableSourceTargetConfig
        returns:
            DAG object
        """        
        self.dag = dag
        self.dag_id = dag_id
        self.description = description
        self.etl_conf = etl_conf

    def create_dag(self) -> DAG:
        # Default settings applied to all tasks
        retry_delay_sec = self.etl_conf.default_args.pop('retry_delay_sec', 300)
        max_retry_delay_sec = self.etl_conf.default_args.pop('max_retry_delay_sec', 1800)
        on_retry_page_slack_alert = self.etl_conf.default_args.pop('on_retry_page_slack_alert', False)
        on_retry_slack_alert = self.etl_conf.default_args.pop('on_retry_slack_alert', False)
        on_failure_page_slack_alert = self.etl_conf.default_args.pop('on_failure_page_slack_alert', False)
        on_failure_slack_alert = self.etl_conf.default_args.pop('on_failure_slack_alert', False)
        end_date = self.etl_conf.dag.get('end_date')

        default_args = { 
            **self.etl_conf.default_args,
            'retry_delay': timedelta(seconds=retry_delay_sec),
            'max_retry_delay': timedelta(seconds=max_retry_delay_sec)
        }

        slack_alert_callback: partial = partial(task_fail_slack_alert, self.etl_conf.connections.get('slack_conn_id'))
        page_slack_alert_callback: partial = partial(task_fail_page_slack_alert, self.etl_conf.connections.get('slack_conn_id'), self.etl_conf.connections.get('pager_conn_id'))

        if on_retry_page_slack_alert:
            default_args['on_retry_callback'] = page_slack_alert_callback
        elif on_retry_slack_alert:
            default_args['on_retry_callback'] = slack_alert_callback

        if on_failure_page_slack_alert:
            default_args['on_failure_callback'] = page_slack_alert_callback
        elif on_failure_slack_alert:
            default_args['on_failure_callback'] = slack_alert_callback

        # Using a DAG context manager, you don't have to specify the dag property of each task
        dag = DAG(
                dag_id=self.dag_id,
                description=self.description,
                start_date=datetime(*map(int, self.etl_conf.dag.get('start_date').split(','))),
                end_date=datetime(*map(int, end_date.split(','))) if end_date else None,
                schedule_interval=self.etl_conf.dag.get('schedule_interval'),
                default_args=default_args,
                max_active_runs=self.etl_conf.dag.get('max_active_runs', 1),
                catchup=self.etl_conf.dag.get('catchup', True),  # enable if you don't want historical dag runs to run
                tags=self.etl_conf.dag_tags)

        return dag


    def add_tasks_to_dag(self, create_dag_class_name: str) -> DAG:
        """
        Adds tasks to DAG object, sets upstream for each task.
        params:
            dag(DAG)
            etl_conf(TableSourceTargetConfig): ETL config object of class TableSourceTargetConfig
        returns:
            dag(DAG) with tasks
        """
        supported_dags = {
            'LoadS3ToRedshift': LoadS3ToRedshift,
            'LoadS3ToRedshiftUsingSensor': LoadS3ToRedshiftUsingSensor,
            'LoadS3ToDatabricks': LoadS3ToDatabricks,
            'LoadS3ToDatabricksUsingSensor': LoadS3ToDatabricksUsingSensor}
        # TODO: Add supported_dags = {'big_query': LoadS3ToBigQuery}
        # # Create parallel pipeline for BiG Query
        # # AWS Lambda to sync s3 bucket with GCS
        # # DAG: gcs_list_operator >> gcs_to_bq >> BigQueryOperator

        dag = supported_dags[create_dag_class_name](
            dag=self.dag, 
            data_source=self.etl_conf.data_source,
            s3_config=self.etl_conf.s3_config, 
            target_config=self.etl_conf.target_config, 
            project_directory=self.etl_conf.project_directory,
            task_id_prefix=self.etl_conf.task_id_prefix,
            is_create_start_end_dummy_task=self.etl_conf.create_start_end_dummy_task,
            is_s3_list_once_per_dag=self.etl_conf.is_s3_list_once_per_dag,
            is_send_slack_success_alerts=self.etl_conf.is_send_slack_success_alerts,
            is_send_slack_failure_alerts=self.etl_conf.is_send_slack_failure_alerts,
            slack_conn_id_success=self.etl_conf.connections.get('slack_conn_id_success'),
            slack_conn_id_failure=self.etl_conf.connections.get('slack_conn_id_failure'),
            sensor_args=self.etl_conf.sensor_args,
            task_args=self.etl_conf.task_args).add_tasks_to_dag()

        return dag

    def create_dag_with_tasks(self, create_dag_class_name: str):
        """
        The actual method that has to be called by a DAG file to get the dag.
        params:
            idem as create_dag + add_tasks_to_dag
        returns:
            DAG object
        """
        if not self.dag:
            self.dag = self.create_dag()
        dag = self.add_tasks_to_dag(create_dag_class_name=create_dag_class_name)
        
        return dag
