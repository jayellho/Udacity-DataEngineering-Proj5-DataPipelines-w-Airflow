from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    json_path = 's3://udacity-dend/log_json_path.json',
    region = "us-west-2",
    delimiter = ",",
    ignore_headers = 1
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    json_path = 'auto',
    region = "us-west-2",
    delimiter = ",",
    ignore_headers = 1
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    target_table = 'songplays',
    insert_columns = 'playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent',
    insert_sql = SqlQueries.songplay_table_insert,
    truncate_table = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table = 'users',
    insert_columns = 'userid, first_name, last_name, gender, level',
    insert_sql = SqlQueries.user_table_insert,
    truncate_table = True    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table = 'songs',
    insert_columns = 'songid, title, artistid, year, duration',
    insert_sql = SqlQueries.song_table_insert,
    truncate_table = True  
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table = 'artists',
    insert_columns = 'artistid, name, location, lattitude, longitude',
    insert_sql = SqlQueries.artist_table_insert,
    truncate_table = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table = 'time',
    insert_columns = 'start_time, hour, day, week, month, year, weekday',
    insert_sql = SqlQueries.time_table_insert,
    truncate_table = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    test_sql_queries = ["SELECT COUNT(*) FROM songs WHERE songid IS NULL", \
                       "SELECT COUNT(*) FROM songs", \
                       "SELECT COUNT(*) FROM songplays", \
                       "SELECT COUNT(*) FROM artists", \
                       "SELECT COUNT(*) FROM artists", \
                       "SELECT COUNT(*) FROM time" \
                      ],
    expected_results = [0,0,0,0,0,0]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator