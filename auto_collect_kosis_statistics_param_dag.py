from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, sys
from pendulum import timezone

sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))
from scripts.auto_collect_kosis_statstics import main

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

local_tz = timezone("Asia/Seoul")

def safe_main(**context):
    import logging
    logger = logging.getLogger("airflow.task")
    try:
        # 🔁 context["params"]로 DAG 파라미터 전달
        execute_date = context["params"].get("execute_date")  # YYYY-MM-DD
        days_back = context["params"].get("days_back")
        logger.info(f"실행 파라미터: execute_date={execute_date}, days_back={days_back}")
        main(execute_date=execute_date, days_back=days_back)
    except Exception as e:
        logger.exception("❌ DAG 실행 중 오류 발생")
        raise

with DAG(
    dag_id='auto_collect_kosis_statistics_param_dag',
    schedule_interval=None,  # ✅ 스케줄러가 실행하지 않음
    start_date=datetime(2024, 1, 1, tzinfo=timezone("Asia/Seoul")),
    catchup=False,
    tags=['kosis', 'manual']
) as dag:

    run_kosis_etl = PythonOperator(
        task_id='run_kosis_statistics_collection',
        python_callable=safe_main,
        provide_context=True,
        params={
            "execute_date": "2025-05-25",
            "days_back": 7
        }
    )
