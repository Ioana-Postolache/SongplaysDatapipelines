from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
# comment out the next 2 lines if you add the s3_bucket variable in Airflow
# from airflow.models import Variable
# s3_bucket = Variable.get('s3_bucket')
s3_bucket = "udacity-dend"
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'max_active_runs': 1,
    # The DAG does not have dependencies on past runs
    'depends_on_past': False,
     # The DAG does not email on retry
    'email_on_retry': False,
    # On failure, the task are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False
}

dag = DAG('dwh_daacleaaarg',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # '0 * * * *' equivalent of @hourly
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    destination_table="public.staging_events",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key="log_data",
    json_format="'s3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_song',
    redshift_conn_id="redshift",
    destination_table="public.staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key="song_data",
    json_format="'auto'",
    provide_context=False,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    destination_table="public.songplays",
    insert_into_table_sql=SqlQueries.songplays_table_insert,  
    check_column="start_time",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.user",
    insert_into_table_sql=SqlQueries.user_table_insert,
    load_mode="delete-load",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.song",
    insert_into_table_sql=SqlQueries.song_table_insert,
    load_mode="delete-load",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.artist",
    insert_into_table_sql=SqlQueries.artist_table_insert,
    load_mode="delete-load",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.time",
    insert_into_table_sql=SqlQueries.time_table_insert,
    check_column="start_time",
    provide_context=True,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    schema = "public",
    tbl_list = ["songplays", "song", "artist", "user", "time"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator