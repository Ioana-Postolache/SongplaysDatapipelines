import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tbl_list="",
                 schema="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tbl_list = tbl_list
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for tbl in self.tbl_list:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.schema}.{tbl}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.schema}.{tbl} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.schema}.{tbl} contained 0 rows")
            logging.info(f"Data quality on table {self.schema}.{tbl} check passed with {records[0][0]} records")