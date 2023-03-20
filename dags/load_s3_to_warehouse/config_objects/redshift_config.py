
class RedshiftConfig(object):
    def __init__(self, redshift_config: dict, data_source: str, table: str) -> None:
        super().__init__()
        templated_dict = {'data_source': data_source, 'table': table}

        self.create_dag_class_name: str = redshift_config.get('create_dag_class_name', 'LoadS3ToRedshift')
        self.redshift_conn_id: str = redshift_config.get('redshift_conn_id')
        self.redshift_aws_conn_id: str = redshift_config.get('redshift_aws_conn_id')

        self.create_dag: bool = redshift_config.get('create_dag') or False
        self.etl_sql_path: str = redshift_config.get('etl_sql_path') or ''
        self.copy_options: list = redshift_config.get('copy_options') or []
        self.copy_columns: list = redshift_config.get('copy_columns') or []
        self.continue_copy_on_error: bool = redshift_config.get('continue_copy_on_error') or False
        self.update_file_name_column: str = redshift_config.get('update_file_name_column') or ''

        self.dest_schema: str = (redshift_config.get('dest_schema') or '').format(**templated_dict)
        self.dest_table: str = (redshift_config.get('dest_table') or '').format(**templated_dict)
        self.staging_schema: str = (redshift_config.get('staging_schema') or '').format(**templated_dict)
        self.staging_table: str = (redshift_config.get('staging_table') or '').format(**templated_dict)

        self.skip_delete_dest_table: bool = redshift_config.get('skip_delete_dest_table') or False
        self.delsert_column: str = redshift_config.get('delsert_column') or ''
        self.delsert_filter: str = redshift_config.get('delsert_filter') or '1 = 1' 
        self.delete_staging_sequence_column: str = redshift_config.get('delete_staging_sequence_column') or ''
        self.upsert_id_columns: list = redshift_config.get('upsert_id_columns') or []
        self.dedupe_order_by_columns: list = redshift_config.get('dedupe_order_by_columns') or []
        self.partition_column: str = redshift_config.get('partition_column') or ''
        self.partition_column_expr: str = (redshift_config.get('transformed_columns') or {}).get(self.partition_column, self.partition_column)        
        self.analyze_table: bool = redshift_config.get('analyze_table') or False
        self.etl_ddl_sql_path: str = (redshift_config.get('etl_ddl_sql_path') or '').format(
                dest_schema=self.dest_schema,
                dest_table=self.dest_table,
                **templated_dict)
        self.etl_staging_ddl_sql_path: str = (redshift_config.get('etl_staging_ddl_sql_path') or '').format(
                staging_schema=self.staging_schema,
                staging_table=self.staging_table,
                **templated_dict)

        self.insert_columns: list = list((redshift_config.get('transformed_columns') or {}).keys())
        self.select_columns: list = list((redshift_config.get('transformed_columns') or {}).values())

        self.additional_join:str = redshift_config.get('additional_join') or ''         
        self.additional_filter:str = redshift_config.get('additional_filter') or '1 = 1' 

        self.is_source_json_format = 'json' in [o.lower() for o in ' '.join(self.copy_options).split(' ')]
