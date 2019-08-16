from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):
    ui_color = '#358140'
    load_sql_template = """
    {create_temp_table_sql}
    {insert_into_table_sql}
        WHERE {check_date_column} BETWEEN {start_date} AND {end_date};
    alter table {destination_table} append from {temp_table};
    drop {temp_table};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) 
                 redshift_conn_id="",
                 destination_table="",
                 insert_into_table_sql="",
                 check_date_column="",
                 start_date="",
                 end_date="",
                 table_type="",
                 temp_table="",
                 create_temp_table_sql,
                 *args, **kwargs):

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.insert_into_table_sql = insert_into_table_sql
        self.destination_table = destination_table
        self.start_date = start_date
        self.end_date = end_date
        self.check_date_column = check_date_column
        self.table_type = table_type
        self.temp_table = temp_table
        self.create_temp_table_sql = create_temp_table_sql

    def execute(self, context):
        pass
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        load_sql = LoadTableOperator.load_sql_template.format(
            insert_into_table_sql = self.insert_into_table_sql,
            start_date = self.start_date,
            end_date  = self.end_date,
            check_date_column = self.check_date_column,
            temp_table = self.temp_table,
            create_temp_table_sql = self.create_temp_table_sql
        )
        if self.table_type == 'dimension':
            self.log.info(f"LoadTableOperator -truncate dimension table: {destination_table}")
            redshift.run(f"TRUNCATE TABLE {destination_table}")
        redshift.run(load_sql)
        self.log.info(f"LoadTableOperator - inserted into table: {destination_table}")