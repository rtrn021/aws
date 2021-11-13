import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.postgres_operator import PostgresOperator

def hello():
    logging.info('Hellooo!!!')


# just name and start_date is mandotory / be default it runs daily
# max_active_runs is for paralel run, if we increase it will run same time for different past months/days
dag1 = DAG('d1',
           start_date=datetime.datetime.now()-datetime.timedelta(days=60),
           end_date=datetime.datetime(2021,12,31,0,0,0,0),
           schedule_interval='@daily',
           max_active_runs=1)


task1 = PythonOperator(task_id='task1',python_callable=hello,dag=dag1,provide_context=True)

# for partitioning we can add where in to sql
# task2 = PostgresOperator(task_id='',dag=dag1,postgres_conn_id='redshift',sql=f"""""")


# To create credentials or variables, Airflow>admin>
# conn_id > aws_credentials
# con type > Amazon Web Services
# login > access key id
# password > secret key id
# provide_context=True > enable using default kwargs in https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html


