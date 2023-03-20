
class DatabricksConfig(object):
    def __init__(self, databricks_config: dict, data_source: str, table: str) -> None:
        super().__init__()
        templated_dict = {'data_source': data_source, 'table': table}

        self.create_dag_class_name: str = databricks_config.get('create_dag_class_name', 'LoadS3ToDatabricks')
        self.databricks_sql_conn_id: str = databricks_config.get('databricks_sql_conn_id')

        self.create_dag: bool = databricks_config.get('create_dag') or False
        self.etl_sql_path: str = databricks_config.get('etl_sql_path') or ''
        self.copy_options: list = databricks_config.get('copy_options') or []
        self.continue_copy_on_error: bool = databricks_config.get('continue_copy_on_error') or False

        self.dest_schema: str = (databricks_config.get('dest_schema') or '').format(**templated_dict)
        self.dest_table: str = (databricks_config.get('dest_table') or '').format(**templated_dict)
        self.staging_schema: str = (databricks_config.get('staging_schema') or '').format(**templated_dict)
        self.staging_table: str = (databricks_config.get('staging_table') or '').format(**templated_dict)

        self.skip_delete_dest_table: bool = databricks_config.get('skip_delete_dest_table') or False
        self.delsert_column: str = databricks_config.get('delsert_column') or ''
        self.delsert_filter: str = databricks_config.get('delsert_filter') or '1 = 1' 
        self.delete_staging_sequence_column: str = databricks_config.get('delete_staging_sequence_column') or ''
        self.upsert_id_columns: list = databricks_config.get('upsert_id_columns') or []
        self.dedupe_order_by_columns: list = databricks_config.get('dedupe_order_by_columns') or []
        self.partition_column: str = databricks_config.get('partition_column') or ''
        self.partition_column_expr: str = (databricks_config.get('transformed_columns') or {}).get(self.partition_column, self.partition_column)        
        self.analyze_table: bool = databricks_config.get('analyze_table') or False
        self.etl_ddl_sql_path: str = (databricks_config.get('etl_ddl_sql_path') or '').format(
                dest_schema=self.dest_schema,
                dest_table=self.dest_table,
                **templated_dict)
        self.etl_staging_ddl_sql_path: str = (databricks_config.get('etl_staging_ddl_sql_path') or '').format(
                staging_schema=self.staging_schema,
                staging_table=self.staging_table,
                **templated_dict)

        self.transformed_columns = databricks_config.get('transformed_columns')
        self.insert_columns: list = list((databricks_config.get('transformed_columns') or {}).keys())
        self.select_columns: list = list((databricks_config.get('transformed_columns') or {}).values())

        self.additional_join:str = databricks_config.get('additional_join') or ''         
        self.additional_filter:str = databricks_config.get('additional_filter') or '1 = 1' 
