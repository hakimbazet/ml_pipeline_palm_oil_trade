from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator


from datetime import datetime



args = {
    'owner': 'user_kappa',
    'start_date': datetime(2018,1,1),
}

dag = DAG(
    dag_id='daily_etl_prediction_palm_oil',
    default_args=args,
    schedule_interval='@daily',
    tags=['daily_schedule']
)


task_1 = BashOperator(
    task_id='load_raw_to_postgres_daily',
    bash_command='python /apps/src/etl_job_daily.py ',
    dag=dag,
)

task_2 = BashOperator(
    task_id='prediction_daily',
    bash_command='python /apps/src/prediction_daily.py ',
    dag=dag,
)

task_1 >> task_2