from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime 
import subprocess as sb



def recuperer():
    sb.run(['python3', '../scripts/recuperer.py'])

def nettoyage():
    sb.run(['python3', '../scripts/nettoyage.py'])
    
def formation():
    sb.run(['python3', '../scripts/formation.py'])

def evaluation():
    sb.run(['python3', '../scripts/evaluation.py'])

with DAG(dag_id="dag1",start_date=datetime(2023,11,6),schedule='@hourly',catchup=True) as dag:
    task1 = PythonOperator (task_id="recuperer",python_callable=recuperer)
    task2 = PythonOperator (task_id="nettoyage",python_callable=nettoyage)
    task3 = PythonOperator (task_id="formation",python_callable=formation)
    task4 = PythonOperator (task_id="evaluation",python_callable=evaluation)
task1 >> task2 >> task3 >> task4