import requests
import json
import os
from dotenv import load_dotenv
import pandas as pd

def fetch_policy_data():
    # .env 파일에서 환경 변수 로드
    dotenv_path = 'C:/Users/kdh20/OneDrive/Desktop/DataBase/API_key/여성가족부_key.env'
    load_dotenv(dotenv_path=dotenv_path)
    service_key = os.getenv('SERVICE_KEY')

    if not service_key:
        raise ValueError("SERVICE_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")

    # API 요청 기본 정보
    url = 'http://apis.data.go.kr/1383000/policy/subjectList'
    api_types = ['youApi', 'equApi', 'famApi', 'proApi', 'othApi']  # 사용할 apiType 목록

    # 결과 데이터를 저장할 리스트 초기화
    all_items = []

    # 각 API 타입과 페이지에 대해 반복
    for api_type in api_types:
        params = {
            'serviceKey': service_key,
            'type': 'json',
            'numOFRows': '10',
            'apiType': api_type
        }
        for page in range(1, 100): ## 1페이지부터 100페이지까지 검색
            params['pageNo'] = page
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # 요청 실패 시 예외 발생
                json_data = response.json()
                items = json_data['body'][0]['items']['item']

                # 각 항목별 데이터를 결과 리스트에 추가
                for item in items:
                    all_items.append({
                        'chrgrNm': item.get('chrgrNm', ''),
                        'deptNm': item.get('deptNm', ''),
                        'title': item.get('title', ''),
                        'regDt': item.get('regDt', ''),
                        'cont' : item.get('cont', ''),
                        'telNo': item.get('telNo', ''),
                        'inqCnt': item.get('inqCnt', ''),
                        'url': item.get('url', '')
                    })
            except (requests.RequestException, ValueError) as e:
                print(f"페이지 {page} 처리 중 오류 발생: {e}")

    # 리스트를 데이터프레임으로 변환
    policy_data = pd.DataFrame(all_items)

    # 데이터프레임 타입 변환
    policy_data['chrgrNm'] = policy_data['chrgrNm'].astype(str)               # 담당자 이름
    policy_data['deptNm'] = policy_data['deptNm'].astype(str)                 # 부서명
    policy_data['title'] = policy_data['title'].astype(str)                   # 제목
    policy_data['regDt'] = pd.to_datetime(policy_data['regDt'], errors='coerce')  # 등록일 (날짜)
    policy_data['cont'] = policy_data['cont'].astype(str)                     # 상세 내용
    policy_data['telNo'] = policy_data['telNo'].astype(str)                   # 전화번호
    policy_data['inqCnt'] = policy_data['inqCnt'].fillna(0).astype(int)       # 조회수
    policy_data['url'] = policy_data['url'].astype(str)                       # URL (고유)
    
    # id 컬럼 추가 (1부터 시작하는 고유 번호)
    policy_data.reset_index(drop=True, inplace=True)
    policy_data.insert(0, 'id', policy_data.index + 1)

    return policy_data


import pymysql

def insert_policy_data_to_db(
    df,
    host='localhost',
    user='root',
    password='password',
    db='my_database',
    table_name='policy_table',
    port=3306
):
    """
    fetch_policy_data()로 얻은 DataFrame을 MySQL에 일괄 삽입하는 함수.
    PyMySQL 사용.

    Parameters
    ----------
    df : pandas.DataFrame
        삽입할 데이터가 들어있는 DataFrame.
        컬럼: [id, chrgrNm, deptNm, title, regDt, cont, telNo, inqCnt, url] 가정
    host : str
        MySQL 서버 주소
    user : str
        MySQL 사용자
    password : str
        MySQL 비밀번호
    db : str
        MySQL DB 이름
    table_name : str
        데이터가 삽입될 테이블 이름
    port : int
        MySQL 포트 (기본 3306)
    """

    # 1) DB 연결
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        charset='utf8mb4'
    )

    try:
        # 2) Cursor 생성
        cursor = connection.cursor()

        # 3) DataFrame -> 튜플 리스트 변환
        #    (id, chrgrNm, deptNm, title, regDt, cont, telNo, inqCnt, url) 순서
        #    주의: df 컬럼 순서를 정확히 일치시켜야 합니다.
        columns = ['id', 'chrgrNm', 'deptNm', 'title', 'regDt', 'cont', 'telNo', 'inqCnt', 'url']
        data_tuples = [tuple(row) for row in df[columns].values]

        # 4) INSERT 쿼리 정의
        #    테이블 column 순서도 위의 columns와 동일하게 맞춘다.
        insert_query = f"""
            INSERT INTO {table_name} (
                id, chrgrNm, deptNm, title, regDt, cont, telNo, inqCnt, url
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 5) executemany()로 일괄 삽입
        cursor.executemany(insert_query, data_tuples)

        # 6) commit
        connection.commit()

        # 필요 시 삽입된 행 수 확인
        print(f"{cursor.rowcount} row(s) inserted into '{table_name}'.")

    except Exception as e:
        # 에러 발생 시 롤백
        connection.rollback()
        print("Error occurred while inserting data:", e)
    finally:
        # 자원 정리
        cursor.close()
        connection.close()

import pandas as pd
if __name__ == "__main__":
       #1) 정책 데이터 수집해서 DataFrame 생성
       policy_df = fetch_policy_data()

       ip_key_path = 'C:/Users/kdh20/OneDrive/Desktop/DataBase/DB_key/ip.env'
       load_dotenv(dotenv_path=ip_key_path)

    # MySQL 접속 정보 불러오기
       db_host = os.getenv("DB_HOST")
       db_user = os.getenv("DB_USER")
       db_password = os.getenv("DB_PASSWORD")

    # 2) DB 삽입
       insert_policy_data_to_db(
        df=policy_df,
        host= db_host,
        user= db_user,
        password= db_password,
        db='app_database',
        table_name='posts'
    )