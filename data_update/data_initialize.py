from spotify_daily_chart import (
    get_access_token, 
    download_csv_files, 
    process_csv_files, 
    download_from_s3
)
from weather_data_extract import get_weather_data
import glob
import json
from pathlib import Path
import boto3
import shutil
import pandas as pd
from datetime import datetime, timedelta
import os
import argparse

# 상수 및 전역 변수
BASE_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIRECTORY = BASE_DIR / "data_update" / "spotify" / "spotify-raw"
OUTPUT_DIRECTORY = BASE_DIR / "data_update" / "spotify" / "spotify-result"

# 유틸리티 함수
def get_secret(key):
    with open(BASE_DIR / 'secrets.json') as f:
        secrets = json.load(f)
    try:
        return secrets[key]
    except KeyError:
        raise EnvironmentError(f"Set the {key} environment variable.")

def upload_to_s3(s3_client, file_paths, s3_path, bucket_name):
    for file_path in file_paths:
        s3_client.upload_file(file_path, bucket_name, f'{s3_path}/{os.path.basename(file_path)}')

# 설정 파일 로드 함수
def load_config():
    with open(BASE_DIR / 'data_update' / 'config.json') as f:
        return json.load(f)

# Spotify 로그인 정보
spotify_email = get_secret('SPOTIFY_EMAIL')
spotify_password = get_secret('SPOTIFY_PASSWORD')

# Spotify API 인증 정보
client_id = get_secret('CLIENT_ID')
client_secret = get_secret('CLIENT_SECRET')

# 작업 전 data_track.csv 다운로드 수행
bucket_name = '7igma-s3'
track_data_s3_path = 'music/track_data/track_data.csv'
track_data_local_path = str(BASE_DIR / 'data_update/track_data.csv')

s3_client = boto3.client(
    's3',
    aws_access_key_id=get_secret('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=get_secret('AWS_SECRET_ACCESS_KEY'),
    region_name=get_secret('AWS_DEFAULT_REGION')
)

def positive_int(value):
    ivalue = int(value)
    if ivalue < 2:
        raise argparse.ArgumentTypeError(f"최소값은 2입니다. 입력된 값: {value}")
    return ivalue

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--past_days', type=positive_int, default=89, help='과거 며칠 동안의 데이터를 가져올지 설정합니다. 최소값은 2입니다.')
    args = parser.parse_args()
    
    # 메인 코드 실행
    access_token = get_access_token(client_id, client_secret)
    headers = {"Authorization": f"Bearer {access_token}"}

    # # 설정 파일 로드
    config = load_config()

    # 기간 및 국가 설정 가져오기
    START_DATE = datetime.now() - timedelta(days=args.past_days)
    END_DATE = datetime.now() - timedelta(days=2)
    COUNTRIES = config['countries']
    COUNTRY_MAP = config['country_map']

    # 날짜 리스트 생성
    dates = []
    current_date = START_DATE
    while current_date <= END_DATE:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    # 다운로드 함수 호출
    download_from_s3(s3_client, bucket_name, track_data_s3_path, track_data_local_path)

    try:
        if os.path.exists(track_data_local_path) and os.path.getsize(track_data_local_path) > 0:
            track_db = pd.read_csv(track_data_local_path, encoding='utf-8-sig')
        else:
            raise FileNotFoundError
    except (FileNotFoundError, pd.errors.EmptyDataError):
        track_db = pd.DataFrame(columns=['track_id', 'duration_ms', 'tempo', 'danceability', 'energy', 'valence'])
        print("track_data.csv가 존재하지 않거나 비어 있어 새로 생성됩니다.")

    # CSV 파일 다운로드 및 처리
    download_csv_files(spotify_email, spotify_password, COUNTRIES, dates, DOWNLOAD_DIRECTORY)
    process_csv_files(DOWNLOAD_DIRECTORY, track_db, headers, OUTPUT_DIRECTORY, COUNTRY_MAP)

    # track_data.csv 파일 덮어쓰기
    try:
        daily_files = [
            str(OUTPUT_DIRECTORY / f) for f in os.listdir(OUTPUT_DIRECTORY) 
            if f.startswith('daily_') and f.endswith('.csv')
        ]
        track_file_paths = [
            str(OUTPUT_DIRECTORY / 'track_data.csv')
        ] if 'track_data.csv' in os.listdir(OUTPUT_DIRECTORY) else []
        upload_to_s3(s3_client, daily_files, 'music/daily', '7igma-s3')
        upload_to_s3(s3_client, track_file_paths, 'music/track_data', '7igma-s3')
    except Exception as e:
        print(f"S3 업로드 오류: {e}")
    finally:
        # 업로드 후 로컬에 저장된 CSV 파일 삭제
        try:
            shutil.rmtree(DOWNLOAD_DIRECTORY)
            shutil.rmtree(OUTPUT_DIRECTORY)
            shutil.rmtree(BASE_DIR / 'data_update' / 'spotify')  # spotify 폴더 삭제
            os.remove(str(BASE_DIR / 'data_update' / 'track_data.csv'))  # 다운로드한 track_data.csv 파일 삭제
        except FileNotFoundError:
            print("삭제할 디렉토리가 없습니다.")

    # 좌표 데이터
    coordinates = config['coordinates']

    # 각 나라에 대해 get_weather_data 함수 실행
    for country, (latitude, longitude) in coordinates.items():
        get_weather_data(args.past_days, country, latitude, longitude)

    daily_file_paths = glob.glob(str(BASE_DIR / 'data_update' / 'weather' / 'daily' / '*'))

    try:
        # CSV 파일을 S3에 업로드
        upload_to_s3(s3_client, daily_file_paths, 'weather/daily', '7igma-s3')
    except Exception as e:
        print(f"S3 업로드 오류: {e}")
    finally:
        # 업로드 후 로컬에 저장된 CSV 파일 삭제
        shutil.rmtree(BASE_DIR / 'data_update' / 'weather' / 'daily')

        # 폴더를 다시 생성
        os.makedirs(BASE_DIR / 'data_update' / 'weather' / 'daily')

    print("모든 작업이 완료되었습니다.")