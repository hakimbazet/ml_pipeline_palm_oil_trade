FROM continuumio/anaconda3

RUN mkdir apps

RUN apt install gcc -y
RUN conda install -c conda-forge xgboost -y
RUN pip install apache-airflow

RUN apt install python-dev -y
RUN apt install libpq-dev -y
RUN pip install psycopg2


RUN airflow initdb

EXPOSE 8080 5002

CMD airflow webserver -p 8080 &> airflow_output.log & airflow scheduler 