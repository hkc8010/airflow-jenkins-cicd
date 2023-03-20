from typing import List
from airflow_common.utils.configuration import get_config
from airflow_common.utils.helpers import get_dict_merge

from airflow_common.dags.load_s3_to_warehouse.config_objects.table_source_target_config import TableSourceTargetConfig


class TableConfig(object):
    def __init__(self, project_directory: str, data_source_config: dict, data_source: str, table: str) -> None:
        super().__init__()
        try:
            table_conf = get_config(config_filename=f'config/{data_source}/tables/{table}.yaml', project_directory=project_directory)
        except FileNotFoundError:
            table_conf = data_source_config.get('tables').get(table)

        table_merge_conf = get_dict_merge(data_source_config, table_conf or {})

        sources_conf = table_merge_conf.pop('sources')
        targets_conf = table_merge_conf.pop('targets')
        sources_default_conf = sources_conf.pop('default')
        targets_default_conf = targets_conf.pop('default')

        sources_merge_conf = {source: get_dict_merge(sources_default_conf, source_conf or {}) for source, source_conf in sources_conf.items()}
        targets_merge_conf = {target: get_dict_merge(targets_default_conf, target_conf or {}) for target, target_conf in targets_conf.items()}

        self.table_source_target_config: List[TableSourceTargetConfig] = [
            TableSourceTargetConfig(project_directory, table_merge_conf, sources_merge_conf.get(target_conf.get('source')), target_conf, data_source, table, target) 
            for target, target_conf in targets_merge_conf.items()]
