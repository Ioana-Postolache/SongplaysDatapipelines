from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#358140'
    load_sql_template = """
    CREATE {temp_table_sql} AS {insert_into_table_sql}
    ALTER TABLE {destination_table} APPEND FROM {temp_table};
    DROP TABLE {temp_table};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) 
                 redshift_conn_id="",
                 destination_table="",
                 insert_into_table_sql="",
                 temp_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.insert_into_table_sql = insert_into_table_sql
        self.destination_table = destination_table
        self.temp_table = temp_table

    def execute(self, context):
        pass
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        load_sql = LoadFactOperator.load_sql_template.format(
            insert_into_table_sql = self.insert_into_table_sql,
            temp_table = self.temp_table
        )
        redshift.run(load_sql)
        self.log.info(f"LoadFactOperator - inserted into table: {destination_table}")