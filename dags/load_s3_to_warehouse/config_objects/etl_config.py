from typing import List
from airflow_common.dags.load_s3_to_warehouse.config_objects.data_source_config import DataSourceConfig


class EtlConfig(object):
    def __init__(self, etl_config: dict) -> None:
        super().__init__()
        self.data_sources_config: List[DataSourceConfig] = [
            DataSourceConfig(etl_config.get('project_directory'), etl_config, data_source) 
            for data_source in etl_config.get('data_sources')]
