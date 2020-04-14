#!/bin/sh

mkdir ~/airflow/dags 

cp daily_schedule_palm_oil.py ~/airflow/dags
cp monthly_schedule_palm_oil.py ~/airflow/dags

python ~/airflow/dags/monthly_schedule_palm_oil.py
python ~/airflow/dags/daily_schedule_palm_oil.py