from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta

class LoadDimensionOperator(BaseOperator):
    ui_color = '#358140'
    load_sql_template = """
     INSERT INTO {destination_table} {insert_into_table_sql}
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) 
                 redshift_conn_id="",
                 destination_table="",
                 insert_into_table_sql="",
                 load_mode="",
                 check_column="",
                 provide_context=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.insert_into_table_sql = insert_into_table_sql
        self.destination_table = destination_table
        self.load_mode = load_mode
        self.check_column = check_column
        self.provide_context=provide_context

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)    
        load_sql = LoadDimensionOperator.load_sql_template.format(
            insert_into_table_sql = self.insert_into_table_sql,
            destination_table = self.destination_table
        )
        
        if(self.load_mode == "delete-load"):
            self.log.info(f"LoadDimensionOperator -truncate dimension table: {self.destination_table}")
            redshift.run(f"TRUNCATE TABLE {self.destination_table}")
        else:
            exec_ts = datetime.strptime(context['ts'].replace("+00:00", ""), '%Y-%m-%dT%H:%M:%S')
            ts = exec_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
            next_ts = (exec_ts + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S.%f")
            load_sql = load_sql + f"WHERE {self.check_column} >= '{ts}' AND {self.check_column} < '{next_ts}'"
        redshift.run(load_sql)
        self.log.info(f"LoadDimensionOperator - inserted into table: {self.destination_table}")