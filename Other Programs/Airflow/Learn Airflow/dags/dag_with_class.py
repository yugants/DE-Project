from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define your class
class MyClass:
    def execute(self):
        print('Hello from MyClass!')
        return 'Hello from MyClass!'

# Create an instance of the class
my_instance = MyClass()

# Define the DAG
with DAG(
        dag_id="dag_with_class",
        schedule_interval="@daily",
        default_args={
            'owner': 'airflow',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2024, 9, 1)
        },
        catchup=False
) as dag:
    # Call the class method using PythonOperator
    execute_class_method = PythonOperator(
        task_id="execute_class_method",
        python_callable=my_instance.execute  # Referencing the class method
    )
