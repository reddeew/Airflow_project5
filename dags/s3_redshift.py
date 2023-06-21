from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator 
from operators.data_quality import DataQualityOperator
from operators.create_schema import CreateSchemaOperator
from helpers import SqlQueries
from helpers import SqlSchema


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 01, 06),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False
}

dag = DAG('s3_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup = False
        )
# Start task
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Task : Redshift table creation
create_schema = CreateSchemaOperator(
    task_id="Create_schema",
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlSchema.create,
    skip=False
)
# Task : copy log data from s3 to redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',   
    region='us-west-2',
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    s3_format="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)
# Task : copying song data from s3 to redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',   
    region='us-west-2',
    table='public.staging_songs',
    s3_bucket='udacity-dend',
    s3_format="JSON 'auto'",
    s3_key='song_data'
)
# Task : Load Songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.songplays',
    sql=SqlQueries.songplays_insert
)
# task : load users to dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.users',
    sql=SqlQueries.users_insert,
    insert_mode='with truncate'
)
# task : load songs to dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.songs_insert,
    table='public.songs',
    insert_mode='with truncate'
)
# task : load artists dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.artists',
    sql=SqlQueries.artists_insert,
    insert_mode='with truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.time',
    sql=SqlQueries.time_insert,
    insert_mode='with truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# tasks Dependencies
start_operator >> create_schema
create_schema >> stage_events_to_redshift >> load_songplays_table
create_schema >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator