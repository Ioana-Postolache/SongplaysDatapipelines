from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
# comment out the next 2 lines if you add the s3_bucket variable in Airflow
# from airflow.models import Variable
# s3_bucket = Variable.get('s3_bucket')
s3_bucket = "airflow-udacity"
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 3),
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

dag = DAG('dwh_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # '0 * * * *' equivalent of @hourly - Run once an hour at the beginning of the hour
          schedule_interval='0 0 1 * *'
          #schedule_interval=None
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
    task_id='Stage_songs',
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
    dag=dag
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.users",
    insert_into_table_sql=SqlQueries.users_table_insert,
    dag=dag
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    task_id='Load_songs_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.songs",
    insert_into_table_sql=SqlQueries.songs_table_insert,
    dag=dag
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    task_id='Load_artists_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.artists",
    insert_into_table_sql=SqlQueries.artists_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    destination_table="public.time",
    insert_into_table_sql=SqlQueries.time_table_insert,
    load_mode="",
    check_column="start_time",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
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