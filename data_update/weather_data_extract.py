import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import os

def get_weather_data(past_days, country, latitude, longitude):
    # Open-Meteo API 호출 (오류 시 재시도 설정)
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # 필요한 모든 기상 변수
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_sum", "wind_speed_10m_max"],
        "past_days": past_days,
        "forecast_days": 0
    }
    responses = openmeteo.weather_api(url, params=params)

    response = responses[0]

    # 일별 데이터 처리
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(3).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(4).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}
    daily_data["country"] = [country] * len(daily_data["date"])
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max

    daily_dataframe = pd.DataFrame(data=daily_data)

    # WMO 코드 변환
    def convert_weather_code_to_wmo(code):
        if 0 <= code <= 19:
            return "Clear"
        elif 20 <= code <= 29:
            return "Very Light Rain"
        elif 30 <= code <= 39:
            return "Strong Wind"
        elif 40 <= code <= 49:
            return "Fog"
        elif 50 <= code <= 59:
            return "Light Rain"
        elif 60 <= code <= 69:
            return "Moderate Rain"
        elif 70 <= code <= 79:
            return "Snow"
        elif 80 <= code <= 99:
            return "Heavy Rain or Snow"
        else:
            return "Unknown"

    daily_dataframe['wmo'] = daily_dataframe['weather_code'].apply(convert_weather_code_to_wmo)

    # 날짜별로 데이터를 그룹화하여 저장
    for date, group in daily_dataframe.groupby('date'):
        date_str = date.strftime('%Y-%m-%d')
        daily_csv_path = f'./weather/daily/{date_str}_weather.csv'
        
        if os.path.exists(daily_csv_path):
            group.to_csv(daily_csv_path, mode='a', header=False, index=False)
        else:
            group.to_csv(daily_csv_path, index=False)

    return daily_dataframe
