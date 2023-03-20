
class S3Config(object):
    def __init__(self, s3_config: dict, data_source: str, table: str) -> None:
        super().__init__()
        templated_dict = {'data_source': data_source, 'table': table}

        self.aws_conn_id: str = s3_config.get('aws_conn_id')

        self.bucket: str = s3_config.get('bucket').format(**templated_dict)
        self.prefixes: list = [prefix.format(**templated_dict) for prefix in s3_config.get('prefixes') or []]
        self.jsonpaths_bucket: str = (s3_config.get('jsonpaths_bucket') or '').format(**templated_dict)
        self.jsonpaths_prefix: str = (s3_config.get('jsonpaths_prefix') or '').format(**templated_dict)
        self.jsonpaths_file: str = (s3_config.get('jsonpaths_file') or '').format(**templated_dict)

        self.manifest_bucket: str = (s3_config.get('manifest_bucket') or '').format(**templated_dict)
        self.manifest_prefix: str = (s3_config.get('manifest_prefix') or '').format(**templated_dict)
        
        self.search_expr: str = (s3_config.get('search_expr') or '').format(**templated_dict)
        self.xcom_file_search_expr: str = (s3_config.get('xcom_file_search_expr') or '').format(**templated_dict)
        self.skip_run_delay_iso_duration: str = s3_config.get('skip_run_delay_iso_duration')
