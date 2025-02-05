import requests
import json
import os
import pandas as pd
import pymysql
from dotenv import load_dotenv

def update_policy_data():
    """
    1) 여성가족부 Open API로부터 정책 데이터를 불러와 DataFrame으로 반환하는 함수.
    2) 각 apiType(youApi, equApi, famApi, proApi, othApi)에 대해 1~2페이지까지 데이터만 추출한다.
    3) Python에서 id 칼럼을 만들지 않는다 (PK는 DB에서 자동부여).
    """

    # .env 파일에서 환경 변수 로드
    dotenv_path = 'C:/Users/kdh20/OneDrive/Desktop/DataBase/API_key/여성가족부_key.env'
    load_dotenv(dotenv_path=dotenv_path)
    service_key = os.getenv('SERVICE_KEY')

    if not service_key:
        raise ValueError("SERVICE_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")

    # API 요청 기본 정보
    url = 'http://apis.data.go.kr/1383000/policy/subjectList'
    api_types = ['youApi', 'equApi', 'famApi', 'proApi', 'othApi']

    # 결과 데이터를 저장할 리스트 초기화
    all_items = []

    # 각 API 타입과 페이지에 대해 반복 (1페이지, 2페이지만 추출)
    for api_type in api_types:
        for page in range(1, 3):
            params = {
                'serviceKey': service_key,
                'type': 'json',
                'numOFRows': '10',
                'apiType': api_type,
                'pageNo': page
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # 요청 실패 시 예외 발생
                json_data = response.json()

                # body -> items -> item 이 존재하는지 확인
                # (없을 수도 있으므로 get으로 안전하게 접근)
                items = json_data.get('body', [{}])[0].get('items', {}).get('item')
                if not items:
                    continue

                # 각 항목별 데이터를 결과 리스트에 추가
                for item in items:
                    all_items.append({
                        'chrgrNm': item.get('chrgrNm', ''),
                        'deptNm': item.get('deptNm', ''),
                        'title': item.get('title', ''),
                        'regDt': item.get('regDt', ''),
                        'cont': item.get('cont', ''),
                        'telNo': item.get('telNo', ''),
                        'inqCnt': item.get('inqCnt', 0),
                        'url': item.get('url', '')
                    })

            except (requests.RequestException, ValueError) as e:
                print(f"[ERROR] {api_type} / 페이지 {page} 처리 중 오류 발생: {e}")

    # 리스트를 데이터프레임으로 변환
    policy_data = pd.DataFrame(all_items)

    # 데이터프레임 컬럼별 타입 변환
    policy_data['chrgrNm'] = policy_data['chrgrNm'].astype(str)
    policy_data['deptNm'] = policy_data['deptNm'].astype(str)
    policy_data['title'] = policy_data['title'].astype(str)
    policy_data['regDt'] = pd.to_datetime(policy_data['regDt'], errors='coerce')
    policy_data['cont'] = policy_data['cont'].astype(str)
    policy_data['telNo'] = policy_data['telNo'].astype(str)
    # 조회수(없으면 0)
    policy_data['inqCnt'] = policy_data['inqCnt'].fillna(0).astype(int)
    policy_data['url'] = policy_data['url'].astype(str)

    return policy_data


def upsert_policy_data_to_db(
    df,
    host='localhost',
    user='root',
    password='password',
    db='my_database',
    table_name='policy_table',
    port=3306
):
    """
    url 칼럼을 UNIQUE KEY로 설정해둔 MySQL 테이블에 대해
    DataFrame 데이터를 Upsert(INSERT ON DUPLICATE KEY UPDATE)로 처리하는 함수.

    Parameters
    ----------
    df : pandas.DataFrame
        삽입 혹은 업데이트할 데이터가 들어있는 DataFrame.
        컬럼: [chrgrNm, deptNm, title, regDt, cont, telNo, inqCnt, url]
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
        #    (chrgrNm, deptNm, title, regDt, cont, telNo, inqCnt, url) 순서
        columns = ['chrgrNm', 'deptNm', 'title', 'regDt', 'cont', 'telNo', 'inqCnt', 'url']
        data_tuples = [tuple(row) for row in df[columns].values]

        # 4) ON DUPLICATE KEY UPDATE 구문
        #    url이 UNIQUE KEY로 잡혀있다고 가정.
        #    url이 중복되면 나머지 필드들을 업데이트한다.
        insert_query = f"""
            INSERT INTO {table_name} (
                chrgrNm, deptNm, title, regDt, cont, telNo, inqCnt, url
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                chrgrNm = VALUES(chrgrNm),
                deptNm  = VALUES(deptNm),
                title   = VALUES(title),
                regDt   = VALUES(regDt),
                cont    = VALUES(cont),
                telNo   = VALUES(telNo),
                inqCnt  = VALUES(inqCnt)
            ;
        """

        # 5) executemany()로 일괄 Upsert
        cursor.executemany(insert_query, data_tuples)

        # 6) commit
        connection.commit()

        # 필요 시 처리된 행 수 확인
        print(f"{cursor.rowcount} row(s) inserted/updated in '{table_name}'.")

    except Exception as e:
        # 에러 발생 시 롤백
        connection.rollback()
        print("[ERROR] Upsert 중 오류 발생:", e)
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    # 1) 정책 데이터 수집해서 DataFrame 생성
    policy_df = update_policy_data()

    ip_key_path = 'C:/Users/kdh20/OneDrive/Desktop/DataBase/DB_key/ip.env'
    load_dotenv(dotenv_path=ip_key_path)

    # MySQL 접속 정보 불러오기
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    # 2) DB Upsert
    upsert_policy_data_to_db(
        df=policy_df,
        host= db_host,
        user= db_user,
        password= db_password,
        db='app_database',
        table_name='posts'
    )