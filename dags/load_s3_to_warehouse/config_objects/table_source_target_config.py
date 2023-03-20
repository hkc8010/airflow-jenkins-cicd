from typing import List
from airflow_common.utils.configuration import get_config
from airflow_common.utils.helpers import get_dict_merge

from airflow_common.dags.load_s3_to_warehouse.config_objects.s3_config import S3Config
from airflow_common.dags.load_s3_to_warehouse.config_objects.redshift_config import RedshiftConfig
from airflow_common.dags.load_s3_to_warehouse.config_objects.databricks_config import DatabricksConfig


class TableSourceTargetConfig(object):
    def __init__(self, project_directory: str, table_conf: dict, source_conf: dict, target_conf: dict, data_source: str, table: str, target: str) -> None:
        super().__init__()
        templated_dict = {'data_source': data_source, 'table': table, 'target': target}

        self.project_directory = project_directory
        self.data_source: str = data_source        
        self.table: str = table
        self.target: str = target

        self.dag: dict = table_conf.get('dag')
        self.dag_tags = []
        if 'tags' in self.dag.keys():
            self.dag_tags = [tag.format(**templated_dict) for tag in self.dag['tags']]
                        
        self.default_args: dict = table_conf.get('default_args')
        self.task_args: dict = table_conf.get('task_args') or {}
        self.connections: dict = table_conf.get('connections')
        self.sensor_args: dict = table_conf.get('sensor_args') or {}  

        self.create_start_end_dummy_task = table_conf.get('create_start_end_dummy_task', False)
        self.is_s3_list_once_per_dag = table_conf.get('is_s3_list_once_per_dag')
        self.task_id_prefix: str = table_conf.get('task_id_prefix').format(**templated_dict)
        self.is_send_slack_success_alerts: bool = table_conf.get('is_send_slack_success_alerts', False)
        self.is_send_slack_failure_alerts: bool = table_conf.get('is_send_slack_failure_alerts', False)
        
        self.s3_config: List[S3Config] = S3Config(source_conf, data_source, table)
        # self.gcs_config = GCSConfig(etl_config.get('gcs_config'))

        if self.target == 'redshift':
            self.target_config: List[RedshiftConfig] = RedshiftConfig(target_conf, data_source, table)
        elif self.target == 'databricks':
            self.target_config: List[DatabricksConfig] = DatabricksConfig(target_conf, data_source, table)            
        # elif self.target == 'big_query':
        # self.big_config = BigQueryConfig(etl_config.get('big_query'))

        self.dag_id: str = table_conf.get('dag').get('dag_id').format(**templated_dict)
        self.description: str = table_conf.get('dag').get('description').format(**templated_dict)
