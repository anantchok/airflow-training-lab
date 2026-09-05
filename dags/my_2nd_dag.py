from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
import logging

default_args = {
    'owner': 'airflow',
}

with DAG (
    'my_2nd_dag',
    schedule_interval='* * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2022, 9, 1),
    tags = ['ETL','Hello World', 'training'],
    catchup=False,
) as dag :

    t1 = DummyOperator(
        task_id='my_1st_task',
        dag=dag,
    )

    t2 = DummyOperator(
        task_id='my_2nd_task',
        dag=dag,
    )

    t3 = DummyOperator(
        task_id='my_3rd_task',
        dag=dag,
    )

    t4 = BashOperator(
        task_id='t4-echo',
        bash_command='echo hello',
        dag=dag
    )

    def say_hello():
        return 'Hello!!!'

    say_hello = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
        dag=dag
    )
    
    def say_bye():
        logging.debug('This is a debug message')
        logging.info('This is an info message')
        logging.warning('This is a warning message')
        logging.error('This is an error message')
        logging.critical('This is a critical message')
        return 'Whatever is returned also gets printed in the logs'
    
    say_bye = PythonOperator(
        task_id='say_bye',
        python_callable=say_bye,
        dag=dag,
    )


    say_hello >> [t1 , t2 ] 
    [t1 , t2 ] >> t3
    t3 >> t4
    t4 >> say_bye