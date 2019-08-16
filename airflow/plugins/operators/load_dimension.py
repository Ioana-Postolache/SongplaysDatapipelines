from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) 
                 redshift_conn_id="",
                 destination_table="",
                 insert_into_table_sql=""
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.insert_into_table_sql = insert_into_table_sql
        self.destination_table = destination_table

    def execute(self, context):
        pass
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        self.log.info(f"LoadDimensionOperator -truncate dimension table: {destination_table}")
        redshift.run(f"TRUNCATE TABLE {destination_table}")
        redshift.run(self.insert_into_table_sql)
        self.log.info(f"LoadDimensionOperator - inserted into table: {destination_table}")