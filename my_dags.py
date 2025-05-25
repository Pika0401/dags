from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
from pendulum import timezone

# scripts 디렉토리 경로 추가
sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))

# 메인 함수 import
from scripts.auto_collect_kosis_statstics import main

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

local_tz = timezone("Asia/Seoul")

with DAG(
    dag_id='auto_collect_kosis_statstics_dag',
    default_args=default_args,
    description='자동 수집된 통계청 KOSIS 데이터를 Oracle DB에 저장',
    schedule_interval='30 13 * * *',  # ✅ UTC 04:30 = KST 13:30
    start_date=datetime(2024, 1, 1, 13, 30, tzinfo=local_tz),  # ✅ tzinfo는 Asia/Seoul
    catchup=False,
    tags=['kosis', 'daily', 'oracle']
) as dag:

    run_kosis_etl = PythonOperator(
        task_id='run_kosis_statistics_collection',
        python_callable=main
    )

    run_kosis_etl.doc = "KOSIS 통계 데이터를 수집하고 Oracle DB에 저장하는 태스크입니다."
