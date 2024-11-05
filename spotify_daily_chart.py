import sys
import os
import requests
import pandas as pd
import time
import random
import base64
import json
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from pathlib import Path
import boto3
import shutil

# 상수 및 전역 변수
BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIRECTORY = BASE_DIR / "spotify" / "spotify-raw"
OUTPUT_DIRECTORY = BASE_DIR / "spotify" / "spotify-result"

# 유틸리티 함수
def get_secret(key):
    with open(BASE_DIR / 'spotify_secrets.json') as f:
        secrets = json.load(f)
    try:
        return secrets[key]
    except KeyError:
        raise EnvironmentError(f"Set the {key} environment variable.")

# AWS 인증 정보 설정
AWS_ACCESS_KEY_ID = get_secret('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = get_secret('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = get_secret('AWS_DEFAULT_REGION')

# S3 클라이언트 초기화
s3_client = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_DEFAULT_REGION)

# Spotify 로그인 정보
spotify_email = get_secret('SPOTIFY_EMAIL')
spotify_password = get_secret('SPOTIFY_PASSWORD')

# Spotify API 인증 정보
client_id = get_secret('CLIENT_ID')
client_secret = get_secret('CLIENT_SECRET')

# 설정 파일 로드 함수
def load_config():
    with open(BASE_DIR / 'spotify_config.json') as f:
        return json.load(f)

# 설정 파일 로드
config = load_config()

# 기간 및 국가 설정 가져오기
START_DATE = datetime.strptime(config['start_date'], "%Y-%m-%d")
END_DATE = datetime.strptime(config['end_date'], "%Y-%m-%d")
COUNTRIES = config['countries']
COUNTRY_MAP = config['country_map']

# 날짜 리스트 생성
dates = []
current_date = START_DATE
while current_date <= END_DATE:
    dates.append(current_date.strftime("%Y-%m-%d"))
    current_date += timedelta(days=1)

# S3에서 파일 다운로드 함수
def download_from_s3(bucket_name, s3_path, local_path):
    try:
        # 파일 존재 여부 확인
        s3_client.head_object(Bucket=bucket_name, Key=s3_path)
        # 파일 다운로드
        s3_client.download_file(bucket_name, s3_path, local_path)
        print(f"{s3_path} 파일이 {local_path}로 다운로드되었습니다.")
    except s3_client.exceptions.NoSuchKey:
        print(f"S3 경로에 {s3_path} 파일이 존재하지 않습니다.")
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"S3 경로에 {s3_path} 파일이 존재하지 않습니다.")
        else:
            print(f"S3 다운로드 오류: {e}")
    except Exception as e:
        print(f"S3 다운로드 중 예외 발생: {e}")

# S3에 파일 업로드 함수
def upload_to_s3(file_paths, s3_path, bucket_name):
    for file_path in file_paths:
        try:
            s3_client.upload_file(file_path, bucket_name, f'{s3_path}/{os.path.basename(file_path)}')
            print(f"{file_path} 파일이 S3의 {s3_path} 폴더에 업로드되었습니다.")
        except Exception as e:
            print(f"{file_path} 업로드 중 오류 발생: {e}")

# 액세스 토큰 요청 함수
def get_access_token(client_id, client_secret):
    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode('utf-8')).decode('ascii')
    headers = {"Authorization": f"Basic {encoded}"}
    payload = {"grant_type": "client_credentials"}

    response = requests.post(endpoint, data=payload, headers=headers)
    if response.status_code == 200:
        access_token = json.loads(response.text)['access_token']
        print(f"액세스 토큰: {access_token}")
        return access_token
    else:
        print(f"토큰 요청 실패: {response.status_code}, {response.text}")
        sys.exit("액세스 토큰을 가져오지 못했습니다.")

# Selenium을 사용한 CSV 파일 다운로드 함수
def download_csv_files(spotify_email, spotify_password, countries, dates, download_directory):
    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": str(download_directory)}
    options.add_experimental_option("prefs", prefs)

    with webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) as driver:
        driver.get("https://charts.spotify.com/charts/view/regional-global-daily/latest")
        time.sleep(2)

        login_button = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/header/div/div[2]/a')
        login_button.click()

        driver.find_element(By.XPATH, '//*[@id="login-username"]').send_keys(spotify_email)
        driver.find_element(By.XPATH, '//*[@id="login-password"]').send_keys(spotify_password)
        driver.find_element(By.XPATH, '//*[@id="login-button"]/span[1]').click()

        time.sleep(5)

        for country in countries:
            for date in dates:
                driver.get(f"https://charts.spotify.com/charts/view/regional-{country}-daily/{date}")
                time.sleep(2)

                try:
                    csv_download_button = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div[3]/div/div/div[2]/span')
                    csv_download_button.click()
                    print(f"{country}의 {date} 데이터 다운로드 완료")
                except Exception as e:
                    print(f"{country}의 {date} 데이터 다운로드 실패: {e}")

# API 요청 및 track_data 업데이트 함수
def fetch_track_info_and_update_db(track_id, headers, track_data):
    print(f"API 함수 호출됨: {track_id}")
    try:
        audio_features_response = requests.get(
            f"https://api.spotify.com/v1/audio-features/{track_id}", headers=headers)

        if audio_features_response.status_code == 200:
            print(f"API 응답 성공: {track_id}")
            track_features = audio_features_response.json()

            track_data.append({
                'track_id': track_id,
                'duration_ms': track_features.get('duration_ms'),
                'tempo': track_features.get('tempo'),
                'danceability': track_features.get('danceability'),
                'energy': track_features.get('energy'),
                'valence': track_features.get('valence')
            })

            time.sleep(random.uniform(1, 2))

        elif audio_features_response.status_code == 429:
            retry_after = int(audio_features_response.headers.get("Retry-After", 5))
            print(f"429 오류 발생. {retry_after}초 후에 재시도합니다.")
            time.sleep(retry_after)

        else:
            print(f"오류 발생: 오디오 특징 {audio_features_response.status_code}")

    except Exception as e:
        print(f"예외 발생: {e}")

# CSV 파일을 통합하고 날짜별 결합, 원본 파일 삭제 함수
def process_csv_files(directory, track_db, headers, output_directory, country_map):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    date_files = {}
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            parts = filename.split('-')
            country = parts[1]
            year = parts[3]
            month = parts[4]
            day = parts[5].split('.')[0].split()[0]
            date = f"{year}-{month}-{day}"

            if date not in date_files:
                date_files[date] = []

            df = pd.read_csv(file_path, encoding='utf-8-sig')

            df['uri'] = df['uri'].str.split(':').str[-1]
            df.rename(columns={'uri': 'track_id'}, inplace=True)
            df['country'] = country_map.get(country, country)
            df['date'] = date

            # API 호출 필요 여부 확인
            new_tracks = df[~df['track_id'].isin(track_db['track_id'])]['track_id'].tolist()

            if new_tracks:
                track_data = []
                for track_id in new_tracks:
                    fetch_track_info_and_update_db(track_id, headers, track_data)

                if track_data:
                    new_track_df = pd.DataFrame(track_data)
                    track_db = pd.concat([track_db, new_track_df], ignore_index=True).drop_duplicates(subset='track_id', keep='last')
                    track_db.to_csv(os.path.join(output_directory, 'track_data.csv'), index=False, encoding='utf-8-sig')
            date_files[date].append(df)
            os.remove(file_path)  # 원본 파일 삭제

    # 날짜별로 파일 결합 후 저장
    for date, dfs in date_files.items():
        combined_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        output_path = os.path.join(output_directory, f"daily_{date}.csv")
        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')

# 메인 코드 실행
access_token = get_access_token(client_id, client_secret)
headers = {"Authorization": f"Bearer {access_token}"}

# 작업 전 data_track.csv 다운로드 수행
bucket_name = '7igma-s3'
s3_path = 'music/track_data/track_data.csv'  
local_path = str(BASE_DIR / 'track_data.csv')

# 다운로드 함수 호출
download_from_s3(bucket_name, s3_path, local_path)

try:
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        track_db = pd.read_csv(local_path, encoding='utf-8-sig')
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
    daily_files = [str(OUTPUT_DIRECTORY / f) for f in os.listdir(OUTPUT_DIRECTORY) if f.startswith('daily_') and f.endswith('.csv')]
    track_file_paths = [str(OUTPUT_DIRECTORY / 'track_data.csv')] if 'track_data.csv' in os.listdir(OUTPUT_DIRECTORY) else []
    upload_to_s3(daily_files, 'music/daily', '7igma-s3')
    upload_to_s3(track_file_paths, 'music/track_data', '7igma-s3')
except Exception as e:
    print(f"S3 업로드 오류: {e}")
finally:
    # 업로드 후 로컬에 저장된 CSV 파일 삭제
    try:
        shutil.rmtree(DOWNLOAD_DIRECTORY)
        shutil.rmtree(OUTPUT_DIRECTORY)
        shutil.rmtree(BASE_DIR / 'spotify')  # spotify 폴더 삭제
        os.remove(str(BASE_DIR / 'track_data.csv'))  # 다운로드한 track_data.csv 파일 삭제
    except FileNotFoundError:
        print("삭제할 디렉토리가 없습니다.")

print("모든 작업이 완료되었습니다.")