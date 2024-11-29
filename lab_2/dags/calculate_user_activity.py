from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.transform_script import transfrom
import pandas as pd


data_dir = '/opt/airflow/dags/data'

dag_default_args = {
  'owner': 'Aleksei Naimushin',
  'depends_on_past': False,
  'start_date': datetime(2024, 1, 1),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG (
  dag_id='calculate_user_activity',
  description="Calculation of user activity for 3 months",
  schedule_interval="@hourly",
  default_args=dag_default_args,
  is_paused_upon_creation=True,
  max_active_runs=1,
  catchup=False
)

start_task = DummyOperator(
  task_id='StartDAG',
  dag=dag,
  owner='Aleksei Naimushin'
)

end_task = DummyOperator(
  task_id='EndDAG',
  dag=dag,
  owner='Aleksei Naimushin'
)

def load_data_from_csv(**kwargs):
  profit_data = pd.read_csv(f'{data_dir}/profit_table.csv')
  ti = kwargs['ti']
  ti.xcom_push(key="profit_data", value=profit_data)

extract_data = PythonOperator(
  dag=dag,
  task_id='ExtractData',
  owner='Aleksei Naimushin',
  python_callable=load_data_from_csv,
  provide_context=True
)

def transform_data(**kwargs):
  ti = kwargs['ti']
  profit_data = ti.xcom_pull(task_ids='ExtractData', key='profit_data')
  # flags_activity = transfrom(profit_data, datetime.today().strftime('%Y-%m-%d'))
  flags_activity = transfrom(profit_data, '2024-03-01')
  ti.xcom_push(key="flags_activity", value=flags_activity)

transform_data = PythonOperator(
  dag=dag,
  task_id='TransformData',
  owner='Aleksei Naimushin',
  python_callable=transform_data,
  provide_context=True
)

def load_data(**kwargs):
  ti = kwargs['ti']
  flags_data = ti.xcom_pull(task_ids='TransformData', key='flags_activity')
  flags_data.to_csv(f'{data_dir}/flags_activity.csv', mode='a', header=False, index=False)

load_data = PythonOperator(
  dag=dag,
  task_id='LoadData',
  owner='Aleksei Naimushin',
  python_callable=load_data,
  provide_context=True
)

start_task >> extract_data >> transform_data >> load_data >> end_task