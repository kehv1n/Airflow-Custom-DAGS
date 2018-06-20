import airflow
import datetime
import os

import trend_load

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta

# import trend_load

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-log-cleanup
START_DATE = datetime(2017, 11, 28)
# BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")
SCHEDULE_INTERVAL = "@daily"    # Change to Noon    # How often to Run. @daily - Once a day at Midnight
DAG_OWNER_NAME = "operations"       # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []          # List of email address to send email alerts to if this job fails
# DEFAULT_MAX_LOG_AGE_IN_DAYS = 10    # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or odler
# ENABLE_DELETE = True                # Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs
NUMBER_OF_WORKERS = 1               # The number of worker nodes you have in Airflow. Will attempt to run this process for however many workers there are so that each worker gets its logs cleared.

default_args = {
    'owner': DAG_OWNER_NAME,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}





dag = DAG(DAG_ID, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, start_date=START_DATE)

t1 = PythonOperator(
    dag = dag,
    task_id = 'json_import',
    provide_context  = True,
    python_callable = trend_load.get_json
)
