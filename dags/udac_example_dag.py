from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import (LoadFactOperator, StageToRedshiftOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          catchup=False,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create Stage Tables
create_stage_events_table = PostgresOperator(
    task_id="create_stage_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_events_table_create
)

create_stage_songs_table = PostgresOperator(
    task_id="create_stage_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_songs_table_create
)

#Copy data from S3 to Redshift Stage Tables
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    copy_options="FORMAT AS JSON 'auto'"
)

# Create Fact Table
create_songplays_table = PostgresOperator(
    task_id="create_songplay_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.songplay_table_create
)

# Load data into Fact Table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.songplay_table_insert
)

# Create Dimension Tables
create_user_table = PostgresOperator(
    task_id="create_user_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.user_table_create
)

create_song_table = PostgresOperator(
    task_id="create_song_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.song_table_create
)

create_artist_table = PostgresOperator(
    task_id="create_artist_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_create
)

create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.time_table_create
)

# Load data into Dimension Tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.user_table_insert,
    table="users",
    mode="append"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.song_table_insert,
    table="songs",
    mode="append"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.artist_table_insert,
    table="artists",
    mode="append"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement=SqlQueries.time_table_insert,
    table="time",
    mode="append"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_statements = [
        {
            "sql": "SELECT COUNT(*) FROM songplays;",
            "operator": "ne",
            "value": 0
        },
        {
            "sql": "SELECT COUNT(*) FROM users;",
            "operator": "ne",
            "value": 0
        },
        {
            "sql": "SELECT COUNT(*) FROM songs;",
            "operator": "ne",
            "value": 0
        },
        {
            "sql": "SELECT COUNT(*) FROM artists;",
            "operator": "ne",
            "value": 0
        },
        {
            "sql": "SELECT COUNT(*) FROM time;",
            "operator": "ne",
            "value": 0
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_stage_events_table
start_operator >> create_stage_songs_table

create_stage_events_table >> stage_events_to_redshift
create_stage_songs_table >> stage_songs_to_redshift

stage_events_to_redshift >> create_songplays_table
stage_songs_to_redshift >> create_songplays_table

create_songplays_table >> load_songplays_table

load_songplays_table >> create_user_table
load_songplays_table >> create_song_table
load_songplays_table >> create_artist_table
load_songplays_table >> create_time_table

create_user_table >> load_user_dimension_table
create_song_table >> load_song_dimension_table
create_artist_table >> load_artist_dimension_table
create_time_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator