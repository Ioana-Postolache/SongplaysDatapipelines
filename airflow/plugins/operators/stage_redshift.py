from airflow.hooks.postgres_hook import PostgresHook    
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.macros import ds_format

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    truncate_copy_sql = """
        TRUNCATE TABLE {};
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON {};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) 
                 redshift_conn_id="",
                 destination_table="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 json_format=",",
                 provide_context=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key        
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format
        self.provide_context=provide_context

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)   
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()           
        
        self.log.info("Copying data from S3 to Redshift")
        if(self.provide_context==True):
            execution_date = context["ds"]
            s3_path = "s3://{}/{}/{}-events.json".format(self.s3_bucket, self.s3_key, ds_format(execution_date, "%Y-%m-%d", "%Y/%m/%Y-%m-%d"))
        else:
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
            
        formatted_sql = StageToRedshiftOperator.truncate_copy_sql.format(
            self.destination_table,
            self.destination_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
        )
        redshift.run(formatted_sql)
        
        self.log.info(f"Finished copying data from {s3_path} to {self.destination_table}")