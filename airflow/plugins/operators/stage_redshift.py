from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    stage_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    {create_table_sql};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) 
                 redshift_conn_id="",
                 destination_table="",
                 create_table_sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql
        self.destination_table = destination_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        stage_sql = StageToRedshiftOperator.stage_sql_template.format(destination_table = self.destination_table, create_table_sql = self.create_table_sql)
        redshift.run(stage_sql)
        self.log.info(f"StageToRedshiftOperator - created stage table: {destination_table}")