import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import boto3
import json
from pathlib import Path
import os
import glob
import shutil

# 상수 및 전역 변수
BASE_DIR = Path(__file__).resolve().parent.parent

# 유틸리티 함수
def get_secret(key):
    with open(BASE_DIR / 'secrets.json') as f:
        secrets = json.load(f)
    try:
        return secrets[key]
    except KeyError:
        raise EnvironmentError(f"Set the {key} environment variable.")

def get_weather_data(country, latitude, longitude):
    # Open-Meteo API 호출 (오류 시 재시도 설정)
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # 필요한 모든 기상 변수
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "precipitation", "weather_code", "cloud_cover", "wind_speed_10m", "wind_direction_10m"],
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_sum", "wind_speed_10m_max"],
        "past_days": 12,
        "forecast_days": 1
    }
    responses = openmeteo.weather_api(url, params=params)

    response = responses[0]

    # 시간별 데이터 처리
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(3).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(4).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(5).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["weather_code"] = hourly_weather_code
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m

    hourly_dataframe = pd.DataFrame(data = hourly_data)

    # 일별 데이터 처리
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(3).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(4).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max

    daily_dataframe = pd.DataFrame(data = daily_data)
    
    hourly_csv_path = f'./weather/hourly/{country}_hourly_weather.csv'
    daily_csv_path = f'./weather/daily/{country}_daily_weather.csv'
    
    # DataFrame을 CSV 파일로 저장
    hourly_dataframe.to_csv(hourly_csv_path, index=False)
    daily_dataframe.to_csv(daily_csv_path, index=False)

    return hourly_dataframe, daily_dataframe

# main.py로 보낼 예정
def upload_to_s3(file_paths, s3_path, bucket_name):
    AWS_ACCESS_KEY_ID = get_secret('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = get_secret('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = get_secret('AWS_DEFAULT_REGION')
    client = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                          region_name=AWS_DEFAULT_REGION
                          )

    for file_path in file_paths:
        client.upload_file(file_path, bucket_name, f'{s3_path}/{os.path.basename(file_path)}')

if __name__ == "__main__":
    # 좌표 데이터
    coordinates = json.load(open('./coordinates.json', 'r'))
    
    # 각 나라에 대해 get_weather_data 함수 실행
    for country, (latitude, longitude) in coordinates.items():
        get_weather_data(country, latitude, longitude)
    
    hourly_file_paths = glob.glob('./weather/hourly/*')
    daily_file_paths = glob.glob('./weather/daily/*')

    try:
        # CSV 파일을 S3에 업로드
        upload_to_s3(hourly_file_paths, 'weather/hourly', '7igma-s3')
        upload_to_s3(daily_file_paths, 'weather/daily', '7igma-s3')
    except Exception as e:
        print(f"S3 업로드 오류: {e}")
    finally:
        # 업로드 후 로컬에 저장된 CSV 파일 삭제
        shutil.rmtree('./weather/hourly')
        shutil.rmtree('./weather/daily')
        
        # 폴더를 다시 생성
        os.makedirs('./weather/hourly')
        os.makedirs('./weather/daily')
