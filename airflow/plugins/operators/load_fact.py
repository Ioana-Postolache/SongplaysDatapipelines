from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta

class LoadFactOperator(BaseOperator):
    ui_color = '#358140'
    # Added WHERE condition so that only data that is between the current ds and the next_ds gets inserted
    load_sql_template = """
     INSERT INTO {destination_table} 
     {insert_into_table_sql}
     WHERE {check_column} >= '{ts}' AND {check_column} < '{next_ts}'
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) 
                 redshift_conn_id="",
                 destination_table="",
                 insert_into_table_sql="",
                 check_column="",
                 provide_context=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.insert_into_table_sql = insert_into_table_sql
        self.destination_table = destination_table
        self.check_column = check_column
        self.provide_context=provide_context

    def execute(self, context):
        exec_ts = datetime.strptime(context['ts'].replace("+00:00", ""), '%Y-%m-%dT%H:%M:%S')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        load_sql = LoadFactOperator.load_sql_template.format(
            insert_into_table_sql = self.insert_into_table_sql,
            destination_table = self.destination_table,
            check_column = self.check_column,
            ts = exec_ts.strftime("%Y-%m-%d %H:%M:%S.%f"),
            next_ts = (exec_ts + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S.%f")
        )
        redshift.run(load_sql)
        self.log.info(f"LoadFactOperator - inserted into table: {self.destination_table}")