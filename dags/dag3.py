from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import numpy as np


output_file = '/tmp/random.txt'
def save_nums ():
   random_num= np.random.randint(1,100 , size=10)
   with open(output_file, 'w') as f:
       for num in random_num:
        f.write(str(num) + '\n')    




with DAG (
    dag_id="dag3",
    start_date=datetime(2025, 9, 30),
    schedule_interval=timedelta(minutes=1),
    catchup=False
) as dag:
    
    task_hello = BashOperator(
        task_id="test_hello",
        bash_command="echo 'Waelcome, Mazen Wael'"
    )

    task_date = BashOperator(
        task_id="test_date",
        bash_command="date"
    )

    task_randomNum = PythonOperator(
        task_id="task_randomNum",
        python_callable= save_nums
    )

task_hello >> task_date >> task_randomNum 