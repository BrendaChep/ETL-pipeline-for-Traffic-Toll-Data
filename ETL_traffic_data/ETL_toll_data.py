#import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG 
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago 
import pandas as pd

#defining DAG arguments
# You can override them on a per-task basis during operator initialization

default_args = {

    'owner' : 'Brighton Kim',
    'start_date' : days_ago(0),
    'email' : ['chep@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),

}

# defining the DAG
# define the DAG

dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1),
)

# define the tasks
# define the first task

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag = dag,
)

# Define the function that the PythonOperator will call
def extract_data():
    # Read the csv file using pandas
    df = pd.read_csv('/home/project/airflow/dags/finalassignment/vehicle-data.csv', 
                 names=["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type", "Number of axles", "Tollplaza code"])

    # Extract the necessary columns
    data = df[["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type"]]
   
    # Save the selected fields to a new csv file
    df.to_csv('/home/project/airflow/dags/finalassignment/csv_data.csv', index=False)

extract_data_from_csv = PythonOperator(
    task_id = 'extract_data_from_csv',
    python_callable=extract_data,
    dag = dag,
)

def extract_data_tsv():
    # Read the tsv file using pandas
    df = pd.read_csv('/home/project/airflow/dags/finalassignment/tollplaza-data.tsv',names=["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type", "Number of axles", "Tollplaza id", "Tollplaza code"], delimiter='\t')
    # Select the required fields
    df = df[['Number of axles', 'Tollplaza id', 'Tollplaza code']]
    # Save the selected fields to a new csv file
    df.to_csv('/home/project/airflow/dags/finalassignment/tsv_data.csv', index=False)

# Create a PythonOperator to call the extract_data_from_tsv function
extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_tsv,
    dag=dag
)

def extract_data_fixed_width():
    # Open the fixed width file
    with open("/home/project/airflow/dags/finalassignment/payment-data.txt", "r") as f:
        lines = f.readlines()
    # Extract the required fields
    extracted_data = []
    for line in lines:
        extracted_data.append([line[:3],line[3:7]])
    # Save the selected fields to a new csv file
    df = pd.DataFrame(extracted_data, columns=["Type of Payment code","Vehicle Code"])
    df.to_csv("/home/project/airflow/dags/finalassignment/fixed_width_data.csv", index=False)

# Create a PythonOperator to call the extract_data_from_fixed_width function
extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_fixed_width,
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d, /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag
)

def transform_data():
    # Read the extracted_data.csv file using pandas
    df = pd.read_csv('/home/project/airflow/dags/finalassignment/extracted_data.csv')
    # Transform the vehicle_type field to uppercase
    df['Vehicle type'] = df['Vehicle type'].str.upper()
    # Save the transformed data to the staging directory
    df.to_csv('/home/project/airflow/dags/finalassignment/staging/transformed_data.csv', index=False)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data