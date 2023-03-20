from airflow_common.utils.configuration import get_config
from airflow_common.dags.load_s3_to_warehouse.config_objects.etl_config import EtlConfig
from airflow_common.dags.load_s3_to_warehouse.dag_factory.dag_factory import DAGFactory


# Load configuration values from the config file
conf = get_config(config_filename='config/config.yaml')
etl_config = EtlConfig(conf)

# Create dags for each data sources --> Tables --> Targets
for data_source in etl_config.data_sources_config:
    for table in data_source.tables_config:
        for table_source_target in table.table_source_target_config:
            if table_source_target.target_config.create_dag:
                dag_id = table_source_target.dag_id
                
                globals()[dag_id] = DAGFactory(
                    dag=globals().get(dag_id),
                    dag_id=dag_id,
                    description=table_source_target.description,
                    etl_conf=table_source_target
                    ).create_dag_with_tasks(table_source_target.target_config.create_dag_class_name)
