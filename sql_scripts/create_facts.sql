-- 0. 데이터베이스 설정
USE DATABASE dev;

-- 1. fact table 생성
CREATE OR REPLACE TABLE analytics.fact_music_daily AS
SELECT
    music_daily.track_id,
    music_daily."date" AS date_id,
    music_daily.rank,
    music_daily.days_on_chart,
    track.duration_ms,
    dim_artist.artist_id,
    dim_location.location_id
FROM raw_data.music_daily AS music_daily
INNER JOIN raw_data.track AS track
    ON music_daily.track_id = track.track_id
INNER JOIN analytics.dim_artist AS dim_artist
    ON SPLIT_PART(music_daily.artist_names, ',', 1) = dim_artist.artist_name
INNER JOIN analytics.dim_location AS dim_location
    ON music_daily.country = dim_location.country_name;

-- 1.1) FK 추가: artist_id
ALTER TABLE analytics.fact_music_daily
ADD CONSTRAINT fk_artist FOREIGN KEY (artist_id)
REFERENCES analytics.dim_artist (artist_id);

-- 1.2) FK 추가: location_id
ALTER TABLE analytics.fact_music_daily
ADD CONSTRAINT fk_location FOREIGN KEY (location_id)
REFERENCES analytics.dim_location (location_id);

-- 2. analytics.fact_weather_daily: 일별 날씨에 대한 테이블(analytics)
CREATE OR REPLACE TABLE analytics.fact_weather_daily AS (
    SELECT
        dim_location.location_id,
        weather_daily."date" AS date_id,
        weather_daily.weather_code AS weather_code_id,
        ROUND(weather_daily.temperature_2m_max, 4) AS temperature_max,
        ROUND(weather_daily.temperature_2m_min, 4) AS temperature_min,
        ROUND(weather_daily.precipitation_sum, 4) AS precipitation_sum,
        ROUND(weather_daily.wind_speed_10m_max, 4) AS wind_speed_max
    FROM raw_data.weather_daily AS weather_daily
    LEFT JOIN analytics.dim_location AS dim_location
        ON weather_daily.country = dim_location.country_name
);

-- 2.1) FK 추가: location_id
ALTER TABLE analytics.fact_weather_daily
ADD CONSTRAINT fk_weather_daily_location_id FOREIGN KEY (location_id)
REFERENCES analytics.dim_location (location_id);

-- 2.2) FK 추가: date_id
ALTER TABLE analytics.fact_weather_daily
ADD CONSTRAINT fk_weather_daily_date_id FOREIGN KEY (date_id)
REFERENCES analytics.dim_date (date_id);

-- 2.3) FK 추가: weather_code_id
ALTER TABLE analytics.fact_weather_daily
ADD CONSTRAINT fk_weather_daily_weather_code_id FOREIGN KEY (weather_code_id)
REFERENCES analytics.dim_weather_code (
    weather_code_id
);
