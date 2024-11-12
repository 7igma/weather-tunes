# 프로젝트 주제

날씨와 음악 선호도간의 상관관계 및 추세 파악 대시보드 구성

# 프로젝트 개요

## 1. 배경

음악은 우리의 감정과 밀접하게 연결되어 있으며, 날씨는 우리의 감정에 큰 영향을 미치는 주요 요소 중 하나입니다. 많은 사람들이 특정 날씨나 기후 조건에 따라 다른 종류의 음악을 즐기며, 이로 인해 음악 취향이 달라질 수 있습니다.

본 프로젝트의 배경은 날씨와 음악 선호도 사이의 연관성을 더 깊이 이해함으로써, 개인의 기분과 현재 환경에 맞춘 더 나은 음악 추천 서비스를 제공하고자 하는 데 있습니다.

## 2. 목표 및 기대효과

본 프로젝트의 목표는 다양한 국가의 음악 스트리밍 데이터를 수집하고, 기후 데이터와 비교하여 특정 날씨 조건이 사람들이 선호하는 음악 장르와 곡에 미치는 영향을 이해하는 것입니다.

이를 통해 사용자의 기분과 현재 환경에 맞는 음악 추천을 제공할 수 있는 방법을 모색하고, 사용자 경험을 향상시킬 수 있는 맞춤형 음악 추천 시스템의 발전에 기여할 수 있을 것으로 기대됩니다.

---

# 프로젝트 세부 내용

## 1. 시스템 아키텍처
![image](https://github.com/user-attachments/assets/749ecc32-3e92-4cc3-a307-fec027623132)

---

## 2. 데이터 수집 및 전처리

### 1) 날씨 데이터 수집

#### 데이터 수집 목표
- 6개국(한국, 일본, 미국, 영국, 호주, 브라질)의 최근 3개월 날씨 데이터 수집
- `weather_data_extract.py` 스크립트

#### 데이터 수집 프로세스

1. **데이터 호출**  
   Open Meteo API를 사용해 각 국가에 대해 `past_days` 동안의 날씨 데이터를 호출

2. **데이터 변환**  
   API로부터 받은 응답 데이터를 NumPy 배열로 변환해 일별 데이터를 처리

3. **DataFrame 생성 및 처리**  
   - Pandas DataFrame을 사용하여 날짜별 국가 정보 및 기상 변수를 추가
   - `convert_weather_code_to_wmo(code)` 함수를 사용하여 기상 코드(WMO 코드)를 변환

#### 데이터 수집 구현 상세  
`get_weather_data(past_days, country, latitude, longitude)`를 정의하여 사용

- **past_days**: 수집할 기간을 설정하는 파라미터 (오늘로부터 n일 전까지)
- **country**: 수집할 국가 이름
- **latitude**: 수집할 국가의 위도
- **longitude**: 수집할 국가의 경도

#### 수집 데이터 정의 및 샘플

> 데이터 정의: 시간별 데이터 (Hourly)

| 변수              | 단위      | 설명                              |
|-------------------|-----------|-----------------------------------|
| temperature_2m    | °C        | 지면에서 2m 높이의 기온          |
| precipitation     | mm        | 강수량                            |
| cloud_cover       | %         | 구름 덮임 정도                    |
| wind_speed_10m    | km/h      | 지면에서 10m 높이의 풍속         |
| wind_direction_10m | °        | 지면에서 10m 높이의 풍향         |
| weather_code      | WMO 코드  | 날씨 상태를 나타내는 WMO 코드    |

> 데이터 예시: 시간별 데이터

| date                    | weather_code | temperature_2m_max | temperature_2m_min | precipitation_sum | wind_speed_10m_max |
|-------------------------|--------------|---------------------|---------------------|-------------------|---------------------|
| 2024-10-23 00:00:00+00:00 | 2            | 14.55              | 6.25               | 0                 | 14.205182           |
| 2024-10-24 00:00:00+00:00 | 0            | 17.05              | 8.3                | 0                 | 7.653705            |
| 2024-10-25 00:00:00+00:00 | 3            | 20.8               | 10.45              | 0                 | 5.0911684           |
| 2024-10-26 00:00:00+00:00 | 2            | 22.05              | 11.9               | 0                 | 6.1305785           |
| 2024-10-27 00:00:00+00:00 | 3            | 16.9               | 9.75               | 0                 | 5.00128             |

> 데이터 정의: 일별 데이터 (Daily)

| 변수                   | 단위      | 설명                              |
|------------------------|-----------|-----------------------------------|
| temperature_2m_max     | °C        | 일 최고 기온                      |
| temperature_2m_min     | °C        | 일 최저 기온                      |
| precipitation_sum      | mm        | 하루 동안의 총 강수량 (비+눈)     |
| weather_code           | WMO 코드  | 날씨 상태를 나타내는 WMO 코드    |
| wind_speed_10m_max     | km/h      | 하루 중 최대 풍속                 |
| wind_gusts_10m_max     | km/h      | 하루 중 최대 돌풍 속도            |

> 데이터 예시: 일별 데이터

| date                    | temperature_2m | precipitation | weather_code | cloud_cover | wind_speed_10m | wind_direction_10m |
|-------------------------|----------------|---------------|--------------|-------------|----------------|---------------------|
| 2024-10-23 00:00:00+00:00 | 11.55         | 0             | 2            | 66          | 14.205182      | 278.74606          |
| 2024-10-23 01:00:00+00:00 | 12.1          | 0             | 2            | 59          | 13.556282      | 280.71307          |
| 2024-10-23 02:00:00+00:00 | 12.6          | 0             | 2            | 53          | 12.57426       | 283.24054          |
| 2024-10-23 03:00:00+00:00 | 13.35         | 0             | 1            | 32          | 11.367109      | 280.954            |
| 2024-10-23 04:00:00+00:00 | 14            | 0             | 1            | 27          | 11.304229      | 279.16226          |

---

### 2) 음악 데이터 수집

#### 데이터 수집 목표
- 6개국(한국, 일본, 미국, 영국, 호주, 브라질)의 최근 3개월 일간 차트 데이터 수집

#### 데이터 수집 프로세스
1. (Amazon S3에 업로드 해놓은 기존 track_data.csv가 있을 경우) track_data.csv 다운로드
    
    → `download_from_s3(bucket_name, s3_path, local_path)`
    
2. Selenium을 활용하여 [Spotify](https://charts.spotify.com/charts/view/regional-global-daily/latest)의 일간 차트 csv 파일 다운로드
    
    → `download_csv_files(sp_email, sp_password, countries, dates, download_directory)`
    
3. Spotify API를 호출하여 상세 트랙 정보가 담긴 track_data.csv 파일 업데이트 (없을 시 생성) 
    
    → `fetch_track_info_and_update_db(track_id, headers, track_data)`
    
4. 일간 차트 csv 파일을 일자별로 통합 및 country, data 컬럼 추가 후, 원본 파일 삭제  
    
    → `process_csv_files(directory, track_db, headers, output_directory, country_map)`
    
5. Amazon S3 버킷에 일간 차트 데이터 및 track_data.csv 업로드

#### 수집 데이터 정의 및 샘플

> 데이터 정의

| 컬럼명              | 설명                                                                                               |
|---------------------|----------------------------------------------------------------------------------------------------|
| **rank**            | 트랙의 현재 차트 순위 (1위부터 200위까지의 순위 제공)                                               |
| **artist_names**    | 트랙의 아티스트 이름, 여러 아티스트가 참여한 경우 쉼표로 구분                                      |
| **track_name**      | 트랙의 제목                                                                                        |
| **source**          | 트랙을 발매한 음반사, 소속사, 또는 프로듀서 등의 정보                                               |
| **peak_rank**       | 트랙이 차트에서 기록한 최고 순위                                                                    |
| **previous_rank**   | 트랙의 이전 차트 순위                                                                              |
| **day_on_chart**    | 트랙이 차트에 등장한 일 수 (며칠 동안 차트에 남아 있었는지를 보여주는 지표)                         |
| **Streams**         | 트랙의 재생 수                                                                                     |
| **track_id**        | 트랙의 고유 ID                                                                                     |
| **artist_popularity** | 아티스트의 인기도를 나타내는 지표 (0에서 100 사이의 값, 값이 클수록 인기가 많음을 의미)          |
| **duration_ms**     | 트랙의 재생 길이 (밀리초 단위)                                                                     |
| **tempo**           | 트랙의 BPM (분당 비트 수), 곡의 빠르기를 측정 (예: 120 BPM은 중간 템포)                              |
| **danceability**    | 트랙의 댄스 가능성을 나타내는 지표 (0에서 1 사이의 값, 값이 높을수록 춤추기 적합)                  |
| **energy**          | 트랙의 에너지 수준 (0에서 1 사이의 값, 값이 높을수록 곡의 에너지가 크며 강한 리듬과 빠른 템포)     |
| **valence**         | 트랙의 감정적 분위기 (0에서 1 사이의 값, 값이 높을수록 밝고 긍정적인 느낌, 낮을수록 슬프고 우울한 느낌) |
| **artist_genres**   | 아티스트의 장르 목록, 아티스트가 연주하는 음악의 장르를 문자열 형태로 나열                         |

> 데이터 정의

| rank | track_id         | artist_names                  | track_name          | source                                                       | peak_rank | previous_rank | days_on_chart | streams | country   | date       |
|------|-------------------|-------------------------------|----------------------|--------------------------------------------------------------|-----------|---------------|---------------|---------|-----------|------------|
| 1    | 5vNRhkKd0yEAg8suGBpjeY | ROSÉ, Bruno Mars              | APT.                | Atlantic Records, THEBLACKLABEL                              | 1         | 1             | 17            | 276,941 | australia | 2024-11-03 |
| 2    | 5G2f63n7IPVPPjfNIGih7Q | Sabrina Carpenter             | Taste               | Island Records                                               | 1         | 2             | 74            | 256,491 | australia | 2024-11-03 |
| 3    | 6dOtVTDdiauQNBQEDOtlAB | Billie Eilish                 | BIRDS OF A FEATHER  | Darkroom/Interscope Records                                  | 1         | 3             | 172           | 246,883 | australia | 2024-11-03 |
| 4    | 7ne4VBA60CxGM75vw0EYad | Gracie Abrams                 | That’s So True      | Gracie Abrams, under exclusive license to Interscope Records | 4         | 5             | 17            | 245,883 | australia | 2024-11-03 |
| 5    | 2plbrEY59IikOBgBGLjaoe | Lady Gaga, Bruno Mars         | Die With A Smile    | Interscope                                                   | 2         | 4             | 80            | 241,443 | australia | 2024-11-03 |


---

### 3) ETL 파이프라인 초기화 모듈

- `data_initialize.py` 파일을 통해 위 기능 함수들을 모아 한 번에 처리
- API KEY 등 민감한 개인정보는 `secrets.json` 파일을 통해 개별 관리
    - .gitignore를 통해 Github repository에는 노출되지 않도록 처리함
- `config.json` 파일을 통해 데이터 수집에 필요한 국가 목록, 좌표 등의 정보를 가져옴
- 수집한 데이터를 로컬에 임시 저장한 뒤, S3 버킷에 업로드
    - 임시 저장한 데이터는 업로드 완료 시 자동 삭제
- argparse를 이용해 데이터 수집 기간 설정 가능 (default=89(일), min = 2(일))

### 4) Docker Container 이용해 일일 데이터 수집 자동화 (+Github Secrets 적용)

- 아래 `Dockerfile` 을 이용해 이미지 생성, LInux 서버에서 Crontab을 이용해 매일 데이터 수집
- Github CLI를 이용해 PAT 로그인을 통해 민감한 정보 보호

```bash
# Python 3.12 이미지 사용
FROM python:3.12-slim

# 작업 디렉토리 설정
WORKDIR /app

# 필수 패키지 업데이트 및 설치 (Selenium, Chrome 및 ChromeDriver 설치를 위해 필요한 도구들)
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    gnupg2 \
    && rm -rf /var/lib/apt/lists/*

# Chromium 설치
RUN apt-get update && apt-get install -y \
    chromium \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI 설치
RUN apt-get update && apt-get install -y curl \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | gpg --dearmor -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update && apt-get install -y gh

# requirements.txt 파일 복사 및 Python 패키지 설치
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# 애플리케이션 코드 복사 (현재 디렉토리의 모든 파일을 컨테이너로 복사)
COPY . .

# GitHub CLI 로그인 및 Organization Secrets 받아와서 secrets.json 파일 생성
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=$GITHUB_TOKEN
RUN gh auth setup-git && \
    echo '{ "SPOTIFY_EMAIL": "$(gh secret get SPOTIFY_EMAIL --org 7igma)", \
            "SPOTIFY_PASSWORD": "$(gh secret get SPOTIFY_PASSWORD --org 7igma)", \
            "CLIENT_ID": "$(gh secret get CLIENT_ID --org 7igma)", \
            "CLIENT_SECRET": "$(gh secret get CLIENT_SECRET --org 7igma)", \
            "AWS_ACCESS_KEY_ID": "$(gh secret get AWS_ACCESS_KEY_ID --org 7igma)", \
            "AWS_SECRET_ACCESS_KEY": "$(gh secret get AWS_SECRET_ACCESS_KEY --org 7igma)", \
            "AWS_DEFAULT_REGION": "$(gh secret get AWS_DEFAULT_REGION --org 7igma)" }' > /app/secrets.json

CMD ["python", "data_initialize.py"]
```

### 5) 데이터 웨어하우스 구조

![image](https://github.com/user-attachments/assets/cd2d5ba7-4fe2-483e-9b12-2a97e71882d0)
- **데이터베이스**
    - 2개로 구분하여 `FEAT1` 은 개발용 DB로 `DEV`는 실제 운영용 DB로 사용될 수 있게 구성.
- **스키마**
    - 네 개 스키마(`RAW_DATA`, `ANLYTICS`, `REPORTING`, `TEST`) 로 구분
    - `RAW_DATA`  Amazon S3에 저장되어 있는 COPY 데이터
    - `ANALYTICS`  분석 기반 가공 데이터
    - `REPORTING`  시각화를 위한 데이터
    - `TEST` 테스트를 위한 임시 스키마
- **테이블** (ANALYTICS 기준)
    - `FACT_MUSIC_DAILY` 스트리밍 차트와 트랙 ID 등의 정보
    - `FACT_WEATHER_DAILY` 최고 기온, 최저 기온, 강수량 등의 날씨 정보
    - `DIM_ARTIST` 아티스트에 대한 정보 (아티스트명에 따른 uuid, 아티스트명)
    - `DIM_DATE`  각 날짜에 대한 연도, 월, 일, 요일 번호, 요일 이름에 대한 정보
    - `DIM_LOCATION` 발매 국가에 대한 코드, 이름, 위/경도, 좌표 등에 대한 정보
    - `DIM_TRACK` 트랙의 장르나 분위기를 파악할 수 있는 메타 데이터(BPM, 에너지 등)
    - `DIM_WEATHER_CODE` WMO ID와 설명에 대한 정보

### 6) 테이블 구조 (ERD)

- `analytics` 스키마
    - **`Star Schema`** 를 사용하여 `Fact` 테이블과 `Dimension` 테이블로 나누고 구조화
    - Fact 테이블은 여러 Dimension 테이블과 연결되어 있음
    - `Fact 테이블`은 수치 정보를 포함하는 중앙 테이블로 음악과 날씨데이터 저장
    
           (fact_music_daily, fact_weather_daily)
    
    - `Dimension 테이블`은 음악과 날씨 데이터의 메타 데이터
    
          (dim_location, dim_artist, dim_track, dim_date, dim_weather_code)
      ![image](https://github.com/user-attachments/assets/4f2ef0f7-b9d5-469e-b82a-e90e51d46019)

### 7) 대시보드 시각화 및 결과
- [Tableau 링크](https://public.tableau.com/app/profile/youngcheon.you/viz/7igma/sheet3?publish=yes)

 ![image](https://github.com/user-attachments/assets/1d360070-df75-409f-b1b8-76e15dbcd5af)

- 지도 시각화를 통해 각 나라에서 가장 많이 청취되는 아티스트를 확인할 수 있습니다.

#### 날씨 조건에 따른 평균 BPM
- **폭우 또는 폭설** 시 가장 높은 BPM(123.72BPM)을 기록하였으며, 이는 날씨가 극단적일 때 활동적인 곡을 선택하는 경향이 있음을 시사합니다.
- **맑은 날씨**(122.99BPM)에도 BPM이 높은 편으로 나타나, 날씨가 좋을 때는 신나는 음악을 선호하는 경향이 보입니다.
- **가벼운 비**나 **보통 비**가 오는 날에는 BPM이 비교적 낮아지며, 차분한 분위기의 곡을 선택하는 경향이 확인됩니다.
- **안개**가 끼는 날은 가장 낮은 BPM(120.74BPM)으로 나타났으며, 이는 차분하고 조용한 곡을 선호함을 나타낼 수 있습니다.

#### 국가별 평균 BPM
- **브라질**은 평균 BPM이 129.27로 가장 높게 나타났으며, 이는 빠르고 활기찬 리듬의 곡을 선호하는 경향이 강함을 시사합니다.
- **일본**이 두 번째로 높은 BPM(127.03BPM)을 기록했으며, 일본 청취자들이 활동적인 음악을 선호하는 것으로 볼 수 있습니다.
- **미국**과 **영국**은 각각 123.37BPM, 123.48BPM을 기록하며 중간 정도의 BPM을 선호하는 경향을 보입니다.
- **호주**의 평균 BPM은 122.49로 다른 국가에 비해 다소 낮은 편이며, 상대적으로 차분한 음악을 선호하는 경향이 확인되었습니다.
- **한국**은 평균 BPM이 120.74로 가장 낮게 나타났으며, 이는 차분하고 감성적인 음악을 선호하는 경향이 강함을 시사합니다. 한국 청취자들이 비교적 여유롭고 느긋한 음악을 선호할 가능성이 있음을 보여줍니다.

![korea-dashboard-1](https://github.com/user-attachments/assets/120b243f-4c2a-4a87-a482-26a55c205ff6)

#### 요일별 감정 비교
 ![image](https://github.com/user-attachments/assets/928618fe-5b20-4812-8590-53bfeeaebf67)

→ 금, 토, 일요일의 평균 감정 점수가 월~목요일보다 높은 경향이 관찰되었습니다. 이는 주말이 다가올수록 사람들의 음악 선호도가 보다 밝고 긍정적인 음악으로 바뀌는 것을 보며 사람들이 주말에 긍정적인 감정을 느끼며 더 활기찬 음악을 선택하는 경향이 있는 것 같아 흥미롭게 느꼈습니다.

#### 날씨 상태와 댄스 가능성
![image](https://github.com/user-attachments/assets/7f7288ef-1db6-42d5-81df-e4a77e93983c)

→ 맑은 날(Clear)에는 다른 흐린 날씨 상태보다 댄스 가능성이 높은 음악이 선호되는 것으로 나타났습니다. 이는 맑은 날씨가 활발한 분위기나 긍적적인 음악을 선택하는 경향을 있다고 볼 수 있습니다.
![korea-dashboard-2](https://github.com/user-attachments/assets/0b02d3ab-8d2e-4d33-87ba-555995f9b4b4)

- 각 나라별 대시보드를 통하여 국가의 날씨, 음악 특성(Tempo, Danceability, Valence, Duration, Energy)을 표현하였습니다.
- 한국의 비가 오는 흐린 날에는 BPM(Tempo)이 다소 낮아지며, 차분한 음악이 선호됩니다.
- 한국 대시보드에서는 맑은 날씨와 주말에 Danceability가 높은 음악이 많이 소비되는 경향이 있습니다.

![auz-dashboard](https://github.com/user-attachments/assets/ff05d59a-2d77-4380-9654-4961cde55e8d)
- 호주 대시보드 분석을 통해 호주 청취자들이 맑은 날씨와 주말에 활기찬 음악을 선호하는 경향을 확인하였으며, 이는 기후와 날씨에 따라 청취자가 선택하는 음악 특성이 달라진다는 것을 시사합니다.
- 호주의 경우 일반적으로 에너지가 높은 음악을 선호합니다. 호주에서는 맑은 날씨가 대부분을 차지하며, 비교적 빠른 템포를 선호하는 경향이 있으며 맑은 날씨일 때 평균 BPM(Tempo)이 높게 나타나 활기찬 음악을 많이 소비하는 경향을 확인할 수 있습니다.

![japan-dashboard](https://github.com/user-attachments/assets/91eda223-3c73-40ac-9358-07b728b46076)
- 일본에서는 맑은 날에 비교적 활기찬 음악을 더 선호하는 것으로 나타났습니다.
- 에너지 지수(Energy)가 높은 곡이 일본 청취자들에게 인기가 많으며, 특히 맑은 날과 주말에 활기찬 음악이 더 많이 소비됩니다. 주말이나 기온이 높을 때 Danceability가 높은 음악이 주로 소비되는 경향이 있습니다.
  
![uk-dashboard](https://github.com/user-attachments/assets/5af8c353-37c9-47cd-8143-3374d675849a)
- 영국은 날씨가 맑은 날보다는 흐린 날이나 비가 오는 날이 많습니다. 이런 날씨에서는 에너지가 낮고 차분한 곡이 주로 소비되며, 맑은 날에는 비교적 활기찬 음악의 소비가 증가하는 경향을 보입니다.
- Energy 수준은 맑은 날에 다소 높은 편이며, 전체적으로 중간 수준의 에너지를 가진 곡들이 고르게 소비되는 경향이 있습니다.
- 영국 청취자들은 날씨가 흐리거나 비가 오는 날에는 차분하고 에너지가 낮은 곡을 선호하지만, 주말이나 맑은 날에는 더 활기찬 음악을 소비하는 패턴을 보입니다.

![brazil-dashboard](https://github.com/user-attachments/assets/4d54f760-1159-4384-87d9-5174c713501f)
- 브라질은 폭우나 극단적인 날씨에는 더 차분하고 에너지가 낮은 곡이 소비되는 모습이 나타납니다.
- 브라질에서는 Danceability가 높은 곡을 일상적으로 선호하는 편이며, 특별한 날씨와 크게 상관없이 일정하게 높은 값을 유지합니다.
- 브라질 청취자들은 날씨 조건에 따라 음악 선택에서의 변화를 보입니다. 특히 폭우나 극단적인 날씨에는 감정 지수가 낮고 에너지가 적은 곡을 선호하는 것으로 분석됩니다.

![usa-dashboard](https://github.com/user-attachments/assets/88ce2063-320f-440b-b808-56b3b1504d96)
- 미국에서는 맑은 날씨에 Danceability가 높고, 에너지와 Valence가 일정하게 유지되는 곡을 선호하는 경향을 보입니다. 비가 오거나 흐린 날씨일 때는 조금 더 차분하고 감정 지수가 낮은 음악이 선호될 가능성이 있습니다.
- 날씨 변화에 큰 영향을 받지 않고 중간 템포부터 빠른 템포까지 고르게 선호하는 것으로 나타났습니다.
- 미국에서는 맑은 날씨에 Danceability와 Energy가 높은 곡을 선호하며, 특히 밝고 활기찬 곡이 자주 소비되는 경향이 있습니다.

---

## 3. 프로젝트 결론

### 1) 대시보드를 통해 도출한 주요 인사이트

**날씨와 BPM의 관계**

날씨 상태에 따라 평균적으로 선호되는 BPM이 다르게 나타났습니다. 예를 들어, 맑은 날에는 댄스 음악의 평균이 높아 활발한 분위기의 음악을 선택하는 경향이 있다고 보고 있습니다.

**국가별 선호 장르 차이**

국가별로 선호하는 음악 장르와 BPM에 차이가 있음을 확인하였으며, 특정 아티스트와 장르가 날씨에 따라 재생 수가 크게 달라지는 것을 파악할 수 있었습니다. 이를 통해 각국의 문화적 특성이 음악 선호도에 영향을 미친다는 것을 알 수 있었습니다.

**시각적 통합**

OpenStreetMap 기반의 지도 시각화는 국가별 주요 아티스트를 한눈에 보여주어, 특정 국가에서 활동하는 아티스트와 그들의 인기도를 쉽게 비교할 수 있었습니다.

### 2) 성취점

**데이터 수집 및 통합**

6개의 국가(한국, 일본, 미국, 영국, 호주, 브라질)의 음악 스트리밍 데이터와 기후 데이터를 성공적으로 수집하고 통합하여, 특정 날씨 조건이 음악 선호도에 미치는 영향을 분석할 수 있었습니다.

### 3) 개선점

**데이터의 다양성 확대**

날씨 데이터는 다양한 문화권과 기후 유형을 포함하도록 확대하고, 음악 데이터는 여러 음악 스트리밍 서비스의 API를 추가적으로 활용하여, 각 지역의 음악 선호도를 더 폭넓게 반영할 필요가 있습니다.

**대시보드 활용성** 

이 대시보드는 기업이 다양한 기후 조건에서의 음악 소비 패턴을 이해하고, 이를 마케팅 전략에 활용할 수 있는 유용한 도구로 사용될 수 있습니다. 예를 들어, 특정 장르나 아티스트를 추천하는 맞춤형 서비스로 확장할 가능성을 보여줍니다. 또한, 기상 데이터와 음악 데이터의 융합이 새로운 인사이트를 제공할 수 있음을 확인하였으며, 향후 더 많은 변수와 지역을 추가하여 보다 깊이 있는 분석을 진행할 수 있을 것입니다.

**클라우드 환경에서의 데이터 수집**

클라우드 환경에서 Selenium을 사용하여 웹 페이지에 접근하려다 보니, 환경 설정이 복잡하고 문제 분석도 어려웠습니다. 특히 클라우드 IP로 웹 페이지에 접속할 경우 봇으로 인식되는 경우가 많아 데이터 수집에 제한이 생겨, 초기 계획과 달리 로컬 서버(컴퓨터)에서 데이터를 수집할 수밖에 없었습니다. 클라우드 환경에서도 잘 동작할 수 있도록 개선 방안을 모색할 필요가 있습니다.

### 4) 한계점

**데이터의 제약**

특정 국가 및 지역에 한정된 데이터 수집으로 인해 모든 기후와 문화적 차이를 충분히 반영하지 못했습니다. 특히 날씨 데이터의 경우 최대 3개월 전까지의 정보만 제공되어, 장기적인 기후 변화를 반영한 분석에는 제한이 있었습니다.

**Spotify의 클라우드 IP 차단 문제**

Spotify의 Charts페이지에서 얻을 수 있는 csv파일을 Selenium을 통해 자동화했으나, 클라우드 환경에서 IP 또는 계정이 차단되거나 봇으로 판단해 데이터를 수집에 제한이 있었습니다.
