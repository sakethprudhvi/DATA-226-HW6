from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

RAW_SCHEMA = Variable.get("RAW_SCHEMA", default_var="raw")
USER_FILE = Variable.get("USER_SESSION_FILE", default_var="user_session_channel.csv")
TS_FILE   = Variable.get("SESSION_TS_FILE",     default_var="session_timestamp.csv")

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_table_load(cursor):
    try:
        cursor.execute("BEGIN")
        cursor.execute('''CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                            userId int not NULL,
                            sessionId varchar(32) primary key,
                            channel varchar(32) default 'direct'  
                            );
                        ''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                            sessionId varchar(32) primary key,
                            ts timestamp  
                            );
                        ''')
        cursor.execute('''CREATE OR REPLACE STAGE raw.blob_stage
                            url = 's3://s3-geospatial/readonly/'
                            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
                        ''')
        cursor.execute('''COPY INTO raw.user_session_channel
                          FROM @raw.blob_stage/user_session_channel.csv; ''')
        cursor.execute('''COPY INTO raw.session_timestamp
                          FROM @raw.blob_stage/session_timestamp.csv; ''')
        
        cursor.execute("COMMIT;")
        
        print("Tables created successfully")
        print("Data loaded successfully")

    except Exception as e:
        cursor.execute("COMMIT;")
        print(e)
        raise e 


with DAG(
    dag_id = 'ETL_DAG',
    start_date = datetime(2024,10,27),
    schedule = '45 2 * * *',
    catchup=False,
    tags=['ETL'],
    description="ETL that creates raw tables and loads CSVs from local Docker volume via Snowflake internal stage",
) as dag:
    cursor = return_snowflake_conn()
    create_table_load(cursor)