from typing import List
from pathlib import Path
from airflow_common.utils.configuration import get_config
from airflow_common.utils.helpers import get_dict_merge
from airflow_common.dags.load_s3_to_warehouse.config_objects.table_config import TableConfig


class DataSourceConfig(object):
    def __init__(self, project_directory:str, etl_config: dict,  data_source: str) -> None:
        super().__init__()
        data_source_config = get_config(config_filename=f'config/{data_source}/base.yaml', project_directory=project_directory)
        data_source_merge_config = get_dict_merge(etl_config, data_source_config or {})
        
        tables_with_config_file = [
            Path(f).stem 
            for f in Path(Path(project_directory) / f'config/{data_source}/tables').glob("*.yaml")]
        tables_from_config = list((data_source_merge_config.get('tables') or {}).keys())
        tables = sorted(set(tables_with_config_file + tables_from_config))

        self.tables_config: List[TableConfig] = [
            TableConfig(project_directory, data_source_merge_config, data_source, table)
            for table in tables]