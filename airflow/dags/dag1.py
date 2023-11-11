from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime 



def hello():
    print("hello")

with DAG(dag_id="sujet1",start_date=datetime(2023,11,11),schedule='@hourly',catchup=True) as dag:
    task1 = PythonOperator (task_id="task1",python_callable=hello)
    task2 = PythonOperator (task_id="task2",python_callable=hello)
    task3 = PythonOperator (task_id="task3",python_callable=hello)
task1 >> task2 >> task3