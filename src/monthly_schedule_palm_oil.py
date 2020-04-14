from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator


from datetime import datetime


args = {
    'owner': 'user_kappa',
    'start_date': datetime(2018,1,1),
}

dag = DAG(
    dag_id='monthly_etl_prediction_palm_oil',
    default_args=args,
    schedule_interval='@monthly',
    tags=['monthly_schedule']
)


task_1 = BashOperator(
    task_id='load_raw_to_postgres_monthly',
    bash_command='python /apps/src/etl_job_monthly.py ',
    dag=dag,
)

task_2 = BashOperator(
    task_id='prediction_monthly',
    bash_command='python /apps/src/prediction_monthly.py ',
    dag=dag,
)

task_1 >> task_2