from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# scripts 디렉토리 경로 추가 (Airflow가 import할 수 있도록)
sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))

# auto_collect_kosis_statstics.py의 main 함수 import
from scripts.auto_collect_kosis_statstics import main

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='auto_collect_kosis_statstics_dag',
    default_args=default_args,
    description='자동 수집된 통계청 KOSIS 데이터를 Oracle DB에 저장',
    schedule_interval='@daily',
    catchup=False,
    tags=['kosis', 'daily', 'oracle']
) as dag:

    run_kosis_etl = PythonOperator(
        task_id='run_kosis_statistics_collection',
        python_callable=main
    )

    run_kosis_etl
