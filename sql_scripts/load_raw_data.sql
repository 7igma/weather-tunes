-- 0. 데이터베이스 설정
USE DATABASE dev;

/*
    1. raw_data 스키마 하위 테이블 생성
*/
CREATE OR REPLACE TABLE raw_data.weather_daily (
    "date" DATE PRIMARY KEY,
    country VARCHAR(32),
    weather_code FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    precipitation_sum FLOAT,
    wind_speed_10m_max FLOAT,
    wmo VARCHAR(32)
);

CREATE OR REPLACE TABLE raw_data.track (
    track_id VARCHAR(128) PRIMARY KEY,
    duration_ms INTEGER,
    tempo FLOAT,
    danceability FLOAT,
    energy FLOAT,
    valence FLOAT
);

CREATE OR REPLACE TABLE raw_data.music_daily (
    rank INTEGER,
    track_id VARCHAR(32) PRIMARY KEY,
    artist_names VARCHAR(256),
    track_name VARCHAR(128),
    source VARCHAR(128),
    peak_rank INTEGER,
    previous_rank INTEGER,
    days_on_chart INTEGER,
    "streams" INTEGER,
    country VARCHAR(32),
    "date" DATE
);

/*
    2. raw_data 스키마 하위 테이블에 데이터 주입(S3 Copy)
*/
COPY INTO raw_data.weather_daily
FROM 's3://<AWS_S3_BUCKET>/weather/daily/'
CREDENTIALS = (AWS_KEY_ID = '<AWS_KEY_ID>' AWS_SECRET_KEY = '<AWS_SECRET_KEY>')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO raw_data.track
FROM 's3://<AWS_S3_BUCKET>/music/track_data/'
CREDENTIALS = (
    AWS_KEY_ID = '<AWS_KEY_ID>'
    AWS_SECRET_KEY = '<AWS_SECRET_KEY>'
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO raw_data.music_daily
FROM 's3://<AWS_S3_BUCKET>/music/daily/'
CREDENTIALS = (AWS_KEY_ID = '<AWS_KEY_ID>' AWS_SECRET_KEY = '<AWS_SECRET_KEY>')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
