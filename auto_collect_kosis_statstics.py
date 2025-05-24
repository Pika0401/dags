"""
@title 통계청 통계표 OpenAPI 자동 수집 ver 2
@author 현동빈 (bwg)
@date 2025-04-30
@description 통계청 KOSIS(OpenAPI) 통계표 데이터를 매일 자동으로 수집하여 Oracle DB에 저장하는 병렬 수집 프로그램

------------------------------------------------------------

■ 목적
- 통계청(KOSIS) OpenAPI를 통해 매일 자동으로 최신 통계표 데이터를 수집
- 수집 결과를 Oracle DB에 저장하여 후속 처리 및 통계 분석 가능
- 수집 완료 여부를 CD_COLLECT_KOSIS_OPENAPI_YN 테이블에 기록하여 추적 가능
- 병렬 처리로 대량 통계표의 빠른 수집 구현

------------------------------------------------------------
■ 주요 흐름
1. kosis_config.ini에서 실행일자, DB 설정, 라이선스 키, 로그 경로 등을 로드
2. 설정된 실행일(execute_date)을 기준으로, 이전 N일간 업데이트된 통계표를 필터링
3. Oracle에서 수집 대상 통계표 목록 조회 (CD_KOSIS_REQ_MPP_P)
   - kosis_config.ini에 지정된 TBL_ID만 필터링 가능 (선택적)
   - 수집 대상 목록 info 로그 출력 및 중복 TBL_ID 자동 경고
4. 각 통계표별 '자료갱신일' 메타정보 요청 (재시도 및 백오프 포함)
5. 자료갱신일이 지정 범위 내인 경우만 수집 대상 선정
6. URL을 생성하고 중복 제거 후, ThreadPoolExecutor를 사용해 병렬 요청 수행
   - 요청 실패 시 최대 10회 재시도, timeout=(120초, 300초)
7. 응답 데이터를 컬럼 정규화, 결측값/비정상값 제거 후 Oracle DB에 청크 단위로 저장
8. 프로그램 실행 전 COMPLETE_YN = 'N', 실행 후 'Y'로 변경
   - 상태 관리 테이블: CD_COLLECT_KOSIS_OPENAPI_YN
9. 전체 수집 건수 및 성공률 로그 출력

------------------------------------------------------------
■ 실행 환경
- Python 3.7 이상
- Oracle Instant Client 설치 및 환경변수 설정 필요
- 의존 패키지: oracledb, pandas, numpy, requests, configparser 등

------------------------------------------------------------
■ 구성 파일
- kosis_config/kosis_config.ini : 실행일자, DB 연결 정보, 라이선스 키, 로그 경로, 필터 TBL_ID 설정
  - 예시:
    execute_date = 2025-05-21
    days_back = 6
    max_workers = 15
    tbl_id = DT_1EA1201, DT_1F02005
- kosis_reader.py : 통계청 OpenAPI 메타 요청 전용 클래스
- kosis_logs/ : 날짜별 info/error 로그 자동 생성 (TimedRotatingFileHandler)

------------------------------------------------------------
■ 주요 함수
- run_kosis_process_logging() : 수집, 정제, 저장 전체 프로세스 실행
- fetch_url() : 단일 URL에 대한 API 요청 및 pandas DataFrame 변환
- upsert_complete_flag() : 상태 관리 테이블에 COMPLETE_YN 플래그 삽입 또는 갱신
- setup_logger() : 일자별 로그 핸들러 생성 및 로그 레벨 설정

------------------------------------------------------------
■ 로깅 및 디버깅
- 수집 대상 통계표 수 및 TBL_ID 목록 info 로그 출력
- 동일한 TBL_ID 중복 존재 시 warning 로그 출력
- 메타 요청 실패 시 최대 5회 재시도 (2, 4, 6, 8, 10초 간격)
- API 요청 실패 시 최대 10회 재시도 (점진적 백오프)
- 응답 데이터에서 OBS_VALUE가 NaN, '-', '...'인 경우 자동 필터링
- 모든 주요 작업은 로그로 기록 (info/error 로그 분리)
- 예외 발생 시 traceback 포함한 logger.error 출력 및 raise 처리

------------------------------------------------------------
■ 성능 최적화 및 설정 유연성
- 병렬 처리 수(max_workers)를 kosis_config.ini로 설정 가능
  - [DEFAULT] 섹션에서 `max_workers = 15` 식으로 지정
  - 설정값은 ThreadPoolExecutor의 동시 요청 수 제한에 사용됨

------------------------------------------------------------
■ 출력 테이블
1. 수집 데이터 저장: CD_KOSTAT_OPENAPI_VAL
   - 컬럼: KOSTAT_TBL_ID, TIME_PERIOD, FREQ, ITM_ID, C1~C8, OBS_VALUE 등
   - 공통 컬럼 자동 설정: Z_REG_*, Z_MOD_*

2. 수집 완료 여부: CD_COLLECT_KOSIS_OPENAPI_YN
   - 프로그램 시작 시 COMPLETE_YN = 'N'으로 초기화
   - 완료 후 COMPLETE_YN = 'Y'로 갱신됨
   - Z_REG_DTM은 최초 실행 시, Z_MOD_DTM은 매 실행 시 업데이트

------------------------------------------------------------
■ 실행 결과 예시
- kosis_logs/kosis_info_20250521.log : 정상 실행 로그
- kosis_logs/kosis_error_20250521.log : 오류 상세 로그
- CD_KOSTAT_OPENAPI_VAL 테이블에 수집된 통계값 반영
- CD_COLLECT_KOSIS_OPENAPI_YN에 수집 상태 'Y'로 기록

------------------------------------------------------------
■ 수집 결과 및 저장 통계
- 각 실행일자별 URL 요청 수 / 응답 수 / 수집 성공률(%) 로그 출력
- 응답 성공 시마다 개별 저장 청크 로그 기록 (start~end index)
- 저장 성공 누적 건수는 최종적으로 info 로그에 출력됨

------------------------------------------------------------
■ 장애 대응 및 상태 플래그 처리
- 메타 정보 및 API 요청 실패 시 최대 재시도 및 백오프 적용
- 수집 시작 전 COMPLETE_YN = 'N'으로 초기화, 완료 후 'Y'로 갱신
- 예외 발생 시 traceback 포함 로그 출력 및 DB rollback 처리
"""


import os
import time
import logging
import requests
import configparser
import concurrent.futures
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from logging.handlers import TimedRotatingFileHandler
import urllib3
import oracledb
import kosis_reader as k_r

urllib3.disable_warnings()

# ✅ 로거 설정 함수
# 로그 파일을 info/error로 분리하고, 날짜별로 파일을 생성하는 설정입니다.
# - 콘솔 출력 핸들러
# - info level 파일 핸들러
# - error level 파일 핸들러
def setup_logger(today: object, log_dir: object) -> object:
    logger = logging.getLogger(f"KOSISLogger_{today}")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', "%Y-%m-%d %H:%M:%S")
        os.makedirs(log_dir, exist_ok=True)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        info_handler = TimedRotatingFileHandler(
            filename=os.path.join(log_dir, f"kosis_info_{today}.log"),
            when="midnight", interval=1, backupCount=30, encoding="utf-8"
        )
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)
        logger.addHandler(info_handler)
        error_handler = TimedRotatingFileHandler(
            filename=os.path.join(log_dir, f"kosis_error_{today}.log"),
            when="midnight", interval=1, backupCount=14, encoding="utf-8"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)
    return logger

# ✅ 공통 컬럼 세팅 함수
# 수집 데이터에 공통 등록/수정 컬럼(Z_*)을 추가하고 현재 시점 기준으로 값을 채웁니다.
# Oracle 테이블의 감사 로그 목적
def set_common_cols(df):
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    df_common_cols = pd.DataFrame(columns=[
        'Z_REG_DTM', 'Z_REGR_ID', 'Z_REG_SCR_ID', 'Z_REG_SVC_ID',
        'Z_MOD_DTM', 'Z_MODR_ID', 'Z_MOD_SCR_ID', 'Z_MOD_SVC_ID'
    ])
    df = pd.concat((df, df_common_cols), axis=1, sort=False)
    fill_date = {'Z_REG_DTM': now, 'Z_MOD_DTM': now}
    fill_id = {'Z_REGR_ID': 'bok', 'Z_REG_SCR_ID': 'python', 'Z_REG_SVC_ID': 'python',
               'Z_MODR_ID': 'bok', 'Z_MOD_SCR_ID': 'python', 'Z_MOD_SVC_ID': 'python'}
    pd.set_option('future.no_silent_downcasting', True)
    df = df.fillna(value=fill_date).fillna(value=fill_id).infer_objects(copy=False)
    return df

# ✅ DB 연결 함수 (재시도 포함)
# oracledb.SessionPool에서 커넥션을 3회까지 재시도하여 획득합니다.
def get_connection_with_retry(pool, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            return pool.acquire()
        except Exception:
            time.sleep(2)

# ✅ KOSIS API URL 생성
# 실행 파라미터를 기반으로 수집 URL을 생성합니다.
# KOSIS API의 사용자 통계 ID(userStatsId) 기반 구성입니다.
def build_kosis_url(license_key, kosis_id, org_id, tbl_id, url_code, prd_se, prd_de):
    return (
        f"https://kosis.kr/openapi/statisticsData.do?method=getList"
        f"&apiKey={license_key}&format=json&jsonVD=Y&userStatsId={kosis_id}/{org_id}/{tbl_id}/2/2/{url_code}"
        f"&prdSe={prd_se}&startPrdDe={prd_de}&endPrdDe={prd_de}"
    ).replace(' ', '')

# ✅ KOSIS API 요청 함수
# URL에 대해 요청을 보내고 JSON 응답을 정규화하여 DataFrame으로 반환합니다.
# - 최대 10회 재시도
# - 실패시 백오프(2, 4, ..., 20초) 적용
def fetch_url(url, logger, max_retries=10):
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"🌐 요청 시도 {attempt}: {url}")
            response = requests.get(url, timeout=(120, 300), verify=False)
            response.raise_for_status()
            logger.info(f"✅ 요청 성공: {url}")  # ✅ 성공 로그 추가
            df = pd.json_normalize(response.json())
            df.rename(columns={
                'PRD_DE': 'TIME_PERIOD',
                'PRD_SE': 'FREQ',
                'TBL_ID': 'KOSTAT_TBL_ID',
                'DT': 'OBS_VALUE'
            }, inplace=True)
            return df.reindex(columns=[
                'KOSTAT_TBL_ID', 'TIME_PERIOD', 'FREQ', 'ITM_ID',
                'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'OBS_VALUE'
            ])
        except Exception as e:
            logger.warning(f"⚠️ 요청 실패 ({attempt}): {url} - {e}")
            time.sleep(attempt * 2)
    logger.error(f"❌ 모든 재시도 실패: {url}")
    return None

# ✅ 수집 상태 플래그 삽입/갱신 함수
# 상태 테이블(CD_COLLECT_KOSIS_OPENAPI_YN)에 수집 여부를 표시합니다.
# - is_init=True일 경우: DELETE 후 INSERT (초기화)
# - is_init=False일 경우: UPDATE only
def upsert_complete_flag(connection, today, complete_yn, is_init=False, logger=None):
    now = datetime.now(ZoneInfo("Asia/Seoul"))

    if is_init:
        # ✅ 최초 1회 초기화: DELETE 후 INSERT
        delete_sql = "DELETE FROM CD_COLLECT_KOSIS_OPENAPI_YN WHERE COLLECT_DATE = :1"
        insert_sql = """
            INSERT INTO CD_COLLECT_KOSIS_OPENAPI_YN (
                COLLECT_DATE, COMPLETE_YN,
                Z_REG_DTM, Z_REGR_ID, Z_REG_SCR_ID, Z_REG_SVC_ID,
                Z_MOD_DTM, Z_MODR_ID, Z_MOD_SCR_ID, Z_MOD_SVC_ID
            ) VALUES (
                :1, :2,
                :3, :4, :5, :6,
                :7, :8, :9, :10
            )
        """
        params = [
            today, complete_yn,
            now, 'bok', 'python', 'python',
            now, 'bok', 'python', 'python'
        ]

        try:
            with connection.cursor() as cursor:
                cursor.execute(delete_sql, [today])
                logger.info(f"🗑️ 기존 COLLECT_DATE = {today} 삭제 완료")

                cursor.execute(insert_sql, params)
                logger.info(f"🆕 COLLECT_DATE = {today}, COMPLETE_YN = '{complete_yn}' 신규 삽입 완료")

                connection.commit()
                logger and logger.info("✅ 트랜잭션 커밋 완료")
        except Exception as e:
            logger.error(f"❌ 상태 초기화 실패: {e}", exc_info=True)
            raise
            # pass
    else:
        # ✅ 이후는 UPDATE만 수행
        update_sql = """
            UPDATE CD_COLLECT_KOSIS_OPENAPI_YN
            SET COMPLETE_YN = :1,
                Z_MOD_DTM = :2
            WHERE COLLECT_DATE = :3
        """
        params = [
            complete_yn, now, today
        ]

        try:
            with connection.cursor() as cursor:
                cursor.execute(update_sql, params)
                logger.info(f"🔄 COMPLETE_YN = '{complete_yn}' 업데이트 완료 (업데이트 행 수: {cursor.rowcount})")
                if cursor.rowcount == 0:
                    logger.warning(f"⚠️ UPDATE 실패: COLLECT_DATE={today} 대상 행 없음")
                connection.commit()
        except Exception as e:
            logger and logger.error(f"❌ 상태 플래그 업데이트 실패: {e}", exc_info=True)
            raise

# ✅ KOSIS 데이터 Oracle DB Insert 함수
# 데이터프레임을 1000건 단위로 나누어 CD_KOSTAT_OPENAPI_VAL 테이블에 저장합니다.
# - 포지셔널 바인딩 방식 (:1 ~ :21)
# - setinputsizes()는 제거되어야 함 (혼용 시 오류 발생)
def insert_kosis_data(df_final: pd.DataFrame, connection, logger):
    """
    Oracle DB에 KOSIS 데이터를 bulk insert (최적화된 array binding 사용)
    """

    insert_sql = """
        INSERT INTO CD_KOSTAT_OPENAPI_VAL (
            KOSTAT_TBL_ID, TIME_PERIOD, FREQ, ITM_ID,
            C1, C2, C3, C4, C5, C6, C7, C8, OBS_VALUE,
            Z_REG_DTM, Z_REGR_ID, Z_REG_SCR_ID, Z_REG_SVC_ID,
            Z_MOD_DTM, Z_MODR_ID, Z_MOD_SCR_ID, Z_MOD_SVC_ID
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12,
            :13, :14, :15, :16, :17, :18, :19, :20, :21
        )
    """

    chunk_size = 1000
    cols = ['KOSTAT_TBL_ID', 'TIME_PERIOD', 'FREQ', 'ITM_ID',
            'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'OBS_VALUE',
            'Z_REG_DTM', 'Z_REGR_ID', 'Z_REG_SCR_ID', 'Z_REG_SVC_ID',
            'Z_MOD_DTM', 'Z_MODR_ID', 'Z_MOD_SCR_ID', 'Z_MOD_SVC_ID']

    saved_count = 0  # ✅ 총 저장 건수 누적 변수
    with connection.cursor() as cursor:
        for start in range(0, len(df_final), chunk_size):
            chunk_df = df_final.iloc[start:start + chunk_size][cols]
            rows = [tuple(row) for row in chunk_df.itertuples(index=False, name=None)]
            try:
                cursor.executemany(insert_sql, rows)
                connection.commit()
                saved_count += len(chunk_df)  # ✅ 저장 건수 누적
                logger.info(f"💾 저장 완료: rows {start} ~ {start + len(chunk_df) - 1}")
            except Exception as e:
                logger.error(f"❌ Insert 실패 (rows {start} ~ {start + len(chunk_df) - 1}): {e}", exc_info=True)
                connection.rollback()
        logger.info(f"✅ 총 저장 건수: {saved_count:,} rows")

# ✅ 메인 수집 실행 함수
# 1. 수집 대상 통계표 목록 조회
# 2. 각 통계표에 대해 자료갱신일 메타 요청
# 3. 갱신일 기준 필터링 (execute_date - days_back ~ execute_date)
# 4. 수집 URL 생성 및 병렬 요청
# 5. 수집된 결과 정제 후 Oracle 저장
# 6. 성공률 통계 및 COMPLETE_YN 상태 갱신
def run_kosis_process_logging(execute_dates, config, today, days_back, pool, logger, max_workers):
    start_time = time.time()
    all_data = []
    date_stats = []  # ✅ 날짜별 수집 통계 저장 리스트 추가
    failed_dates = []  # 💥 DB 저장 실패 일자 저장용
    logger.info(f"✅ KOSIS 수집 프로세스 시작 | 대상일자: {execute_dates}")

    connection = get_connection_with_retry(pool)
    logger.info("🔗 Oracle DB 연결 성공")

    license_key = config.get("KOSIS", "license_key")
    kosis_id = config.get("KOSIS", "kosis_id")

    for execute_date in execute_dates:
        logger.info(f"🟡 수집 시작: {execute_date}")

        cursor = connection.cursor()

        # kosis_config.ini에서 필터용 TBL_ID 목록 불러오기
        tbl_id_raw = config.get("DEFAULT", "tbl_id", fallback="").strip()
        tbl_id = [tbl.strip() for tbl in tbl_id_raw.split(',') if tbl.strip()]

        # SQL 조건절 조립
        if tbl_id:
            # 바인딩 가능한 IN 조건 생성 (:1, :2, ...)
            placeholders = ','.join([f':{i + 1}' for i in range(len(tbl_id))])
            sql = f"""
                SELECT ORG_ID, TBL_ID, URL
                FROM CD_KOSIS_REQ_MPP_P
                WHERE URL IS NOT NULL AND TBL_ID IN ({placeholders})
            """
            cursor.execute(sql, tbl_id)
            logger.info(f"🔎 TBL_ID 필터 적용됨: {tbl_id}")
        else:
            sql = "SELECT ORG_ID, TBL_ID, URL FROM CD_KOSIS_REQ_MPP_P WHERE URL IS NOT NULL"
            cursor.execute(sql)

        # 결과 → DataFrame
        df_org_tbl = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        dup_check = df_org_tbl.duplicated(subset=['TBL_ID'], keep=False)
        if dup_check.any():
            dup_list = df_org_tbl[dup_check]['TBL_ID'].drop_duplicates().tolist()
            logger.warning(f"⚠️ 중복된 TBL_ID 존재 ({len(dup_list)}개): {dup_list}")

        # ✅ 이후 실제 처리용으로는 완전 중복 제거
        df_org_tbl = df_org_tbl.drop_duplicates(subset=['TBL_ID', 'ORG_ID', 'URL'])

        cursor.close()

        logger.info(f"📋 수집 대상 통계표 수: {len(df_org_tbl)}개")
        tbl_ids = df_org_tbl['TBL_ID'].tolist()
        logger.info("📄 수집 대상 목록:")
        for i, tbl_id in enumerate(tbl_ids, 1):
            logger.info(f"{i}. {tbl_id}")

        api = k_r.Kosis(license_key)
        results = []

        for idx, row in df_org_tbl.iterrows():
            logger.debug(f"🔍 메타정보 요청 [{idx + 1}/{len(df_org_tbl)}]: ORG_ID={row['ORG_ID']} / TBL_ID={row['TBL_ID']}")
            for attempt in range(1, 6):
                try:
                    meta = api.get_data(
                        service_name='통계표설명',
                        detail_service_name='자료갱신일',
                        orgId=row['ORG_ID'],
                        tblId=row['TBL_ID']
                    )
                    meta['org_id'], meta['tbl_id'], meta['col_url'] = row['ORG_ID'], row['TBL_ID'], row['URL']
                    results.append(meta)
                    logger.debug(f"✅ 메타정보 요청 성공 [{idx + 1}/{len(df_org_tbl)}]")
                    break
                except Exception as e:
                    logger.warning(f"⚠️ 메타 요청 실패 (시도 {attempt}) [{idx + 1}/{len(df_org_tbl)}]: {e}")
                    time.sleep(attempt * 2)

        if not results:
            logger.warning(f"❌ 메타 정보 없음: {execute_date}")
            continue


        df_meta = pd.concat(results, ignore_index=True)

        if df_meta.empty:
            logger.warning(f"⚠️ 필터링 후 데이터 없음: {execute_date}")
            continue

        # 날짜 형식 변환
        exec_date_obj = datetime.strptime(execute_date, "%Y-%m-%d")

        # 💡 실행일 기준으로 days_back일 전부터 포함 (예: 2025-05-20 기준 6일 전 → 2025-05-14 포함)
        start_date = (exec_date_obj - timedelta(days=days_back)).strftime("%Y-%m-%d")  # 일주일 전
        end_date = execute_date  # 실행일자까지 포함

        # 필터링 조건 변경 (자료갱신일이 문자열로 되어 있다고 가정)
        # ✅ 자료갱신일이 지정된 범위 내인 통계표만 필터링
        df_meta = df_meta[
            (df_meta['자료갱신일'] >= start_date) &
            (df_meta['자료갱신일'] <= end_date)
            ]

        logger.info(f"📌 갱신일자 일치 통계표 수: {len(df_meta)}")
        # print(df_meta)

        freq_map = {"월": "M", "반기": "S", "년": "Y", "분기": "Q"}
        df_meta["수록주기"] = df_meta["수록주기"].map(freq_map)

        url_list = [
            build_kosis_url(license_key, kosis_id, row['org_id'], row['tbl_id'],
                            row['col_url'], row['수록주기'], row['수록시점'])
            for _, row in df_meta.iterrows()
        ]
        url_list = list(set(filter(None, url_list)))
        logger.info(f"🌐 데이터 수집 URL 수: {len(url_list)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(fetch_url, url, logger): url for url in url_list}
            # execute_date 단위로 처리 & DB 즉시 저장
            day_data = []
            for future in concurrent.futures.as_completed(future_to_url):
                df = future.result()
                if df is not None:
                    day_data.append(df)

            if day_data:
                df_final = pd.concat(day_data, ignore_index=True)
                df_final = df_final.replace({np.nan: None})
                df_final = df_final[df_final['OBS_VALUE'].notna()]
                df_final = df_final[~df_final['OBS_VALUE'].isin(['-', '...'])]
                df_final['OBS_VALUE'] = pd.to_numeric(df_final['OBS_VALUE'], errors='coerce')
                df_final = df_final.dropna(subset=['KOSTAT_TBL_ID'])

                df_final = set_common_cols(df_final)
                logger.info(f"📦 [{execute_date}] 정제된 데이터 수: {len(df_final)}")

                insert_kosis_data(df_final, connection, logger)
            else:
                logger.warning(f"⚠️ 수집 데이터 없음: {execute_date}")

        # ✅ 날짜별 통계 저장
        date_stats.append({
            "date": execute_date,
            "url_count": len(url_list),
            "success_count": len(day_data)
        })

        # ✅ 대체: 수집이 전혀 없을 때 경고만 남김
        if not date_stats:
            logger.warning("⚠️ 전체 기간 동안 수집된 데이터가 없습니다. DB 저장 및 상태 플래그 스킵됩니다.")

    elapsed = round(time.time() - start_time, 2)
    minutes = int(elapsed // 60)
    seconds = round(elapsed % 60, 2)

    logger.info(f"🏁 전체 수집 완료 | 총 소요시간: {minutes}분 {seconds}초")

    # ✅ 날짜별 수집 통계 요약 출력
    if date_stats:
        logger.info("📊 날짜별 수집 성공률 통계")
        for stat in date_stats:
            rate = round(stat['success_count'] / stat['url_count'] * 100, 2) if stat['url_count'] else 0.0
            logger.info(
                f"📅 {stat['date']} | URL 수: {stat['url_count']} | 성공 수: {stat['success_count']} | 성공률: {rate:.2f}%")

    upsert_complete_flag(connection, today, 'Y', is_init=False, logger=logger)
    logger.info("📍 상태 플래그 (Y) 저장 완료")
    connection.close()
    logger.info("🔌 Oracle DB 연결 종료")

# ✅ main 함수
# kosis_config.ini 설정 불러오기, 날짜 계산, 로거 설정, DB pool 생성
# 초기 COMPLETE_YN = 'N' 설정 후 수집 프로세스 실행
def main():

    config = configparser.ConfigParser()
    config.read("kosis_config/config.ini", encoding="utf-8")

    execute_raw = config.get("DEFAULT", "execute_date", fallback="")
    base_date = datetime.strptime(execute_raw.strip(), "%Y-%m-%d") if execute_raw else datetime.now()

    today = datetime.now().strftime("%Y%m%d")  # YYYYMMDD 형식
    log_dir = config.get("DEFAULT", "log_dir")

    max_workers_str = config.get("DEFAULT", "max_workers", fallback="10").strip()
    max_workers = int(max_workers_str) if max_workers_str else 15

    # ✅ 로거 먼저 세팅해야 이후 logging 가능
    logger = setup_logger(today, log_dir)

    execute_dates = [
        (base_date - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in reversed(range(1))
    ]

    # 💡 실행일 기준으로 days_back일 전부터 포함 (예: 2025-05-20 기준 6일 전 → 2025-05-14 포함)
    days_back_str = config.get("DEFAULT", "days_back", fallback="6").strip()
    days_back = int(days_back_str) if days_back_str else 6

    # ✅ Oracle DB 연결 (최초 1회)
    pool = oracledb.SessionPool(
        user=config.get("DB", "user"),
        password=config.get("DB", "password"),
        dsn=config.get("DB", "dsn"),
        min=config.getint("DB", "min"),
        max=config.getint("DB", "max"),
        increment=config.getint("DB", "increment"),
        encoding=config.get("DB", "encoding")
    )
    connection = get_connection_with_retry(pool)

    # ✅ 최초 1회 상태 초기화 (COMPLETE_YN = 'N', Z_REG_DTM 포함)
    upsert_complete_flag(connection, today, 'N', is_init=True, logger=logger)
    logger.info("📍 상태 초기화 완료 (COMPLETE_YN = 'N', Z_REG_DTM 최신화)")
    connection.close()

    run_kosis_process_logging(execute_dates, config, today, days_back, pool, logger, max_workers)


if __name__ == "__main__":
    main()
