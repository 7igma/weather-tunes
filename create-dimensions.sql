-- 0.dev 데이터베이스를 사용
USE DATABASE dev;

-- 0.1) analytics 스키마로 변경
USE SCHEMA dev.analytics;

-- 1. dim_artist 테이블 생성
CREATE OR REPLACE TABLE dim_artist (
    artist_id VARCHAR(36) PRIMARY KEY DEFAULT UUID_STRING(),
    artist_name VARCHAR(256)
);

-- 1.1) artist_name 데이터 삽입
INSERT INTO dim_artist(artist_name)
    SELECT DISTINCT SPLIT_PART(artist_names, ',', 1) AS artist_name
    FROM raw_data.music_daily;
    

    
-- 2. dim_weather_code 테이블 생성
CREATE OR REPLACE TABLE dim_weather_code AS
SELECT DISTINCT weather_code AS weather_code_id, wmo AS description
FROM raw_data.weather_daily
ORDER BY weather_code_id;

-- 2.1) PK 추가: weather_code_id
ALTER TABLE dim_weather_code ADD CONSTRAINT dim_weather_code_pk PRIMARY KEY (weather_code_id);



-- 3. dim_location 테이블 생성
CREATE TABLE dim_location (
    location_id VARCHAR(36) PRIMARY KEY DEFAULT UUID_STRING(),
    country_code VARCHAR(2),
    country_name VARCHAR(100),
    city VARCHAR(100),
    longitude FLOAT,
    latitude FLOAT
);

-- 3.1) 데이터 삽입
INSERT INTO analytics.dim_location (country_code, country_name, city, longitude, latitude) 
VALUES 
    ('kr', 'korea', 'seoul', 126.978, 37.5665),
    ('jp', 'japan', 'tokyo', 139.8395, 35.6528),
    ('us', 'usa', 'washington', -77.0363, 38.8951),
    ('gb', 'uk', 'london', -0.1278, 51.5074),
    ('au', 'australia', 'canberra', 149.1281, -35.2835),
    ('br', 'brazil', 'brasilia', -47.8828, -15.7939);

    

-- 4. dim_date 테이블 생성
CREATE OR REPLACE TABLE dim_date AS
SELECT 
    "date" AS date_id,
    EXTRACT(YEAR FROM TO_DATE("date")) AS year,           
    EXTRACT(MONTH FROM TO_DATE("date")) AS month,         
    EXTRACT(DAY FROM TO_DATE("date")) AS day,             
    EXTRACT(DOW FROM TO_DATE("date")) AS day_of_week_num,
    CASE 
        WHEN EXTRACT(DOW FROM TO_DATE("date")) = 0 THEN 'Sun'
        WHEN EXTRACT(DOW FROM TO_DATE("date")) = 1 THEN 'Mon'
        WHEN EXTRACT(DOW FROM TO_DATE("date")) = 2 THEN 'Tue'
        WHEN EXTRACT(DOW FROM TO_DATE("date")) = 3 THEN 'Wed'
        WHEN EXTRACT(DOW FROM TO_DATE("date")) = 4 THEN 'Thu'
        WHEN EXTRACT(DOW FROM TO_DATE("date")) = 5 THEN 'Fri'
        WHEN EXTRACT(DOW FROM TO_DATE("date")) = 6 THEN 'Sat'
    END AS day_of_week_name
FROM raw_data.weather_daily
GROUP BY date_id
ORDER BY date_id;

-- 4.1) PK 추가: date_id
ALTER TABLE dim_date ADD CONSTRAINT dim_date_pk PRIMARY KEY (date_id);



-- 5. dim_track 테이블 생성
CREATE OR REPLACE TABLE analytics.dim_track AS (
    SELECT
        DISTINCT track.track_id,
        music_daily.track_name,
        track.duration_ms,
        track.tempo,
        track.danceability,
        track.energy,
        track.valence
    FROM raw_data.track AS track
    LEFT JOIN raw_data.music_daily AS music_daily
        ON track.track_id = music_daily.track_id
);


-- 5.1) PK 추가: track_id
ALTER TABLE analytics.dim_track ADD CONSTRAINT pk_dim_track_temp PRIMARY KEY (track_id);