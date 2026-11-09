# Docker Official Image maintained by the Apache Spark developers.
FROM apache/spark:4.2.0

USER root

# NLTK 및 Hugging Face 모델 캐시 디렉터리 경로 환경 변수
ENV NLTK_DATA=/opt/nltk_data
ENV HF_HOME=/opt/hf_cache
ENV PYTHONUNBUFFERED=1

# 종속 패키지 설치: PyTorch CPU 경량 버전 우선 설치 (ARM64/Mac 환경에서 10GB+ 불필요한 CUDA 라이브러리 유입 및 디스크 용량 부족 [Errno 28] 방지)
RUN python3 -m pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

# NLTK VADER 사전 및 Hugging Face 경량 제품 리뷰 모델(DistilBERT) 가중치 사전 빌드/캐싱
RUN mkdir -p /opt/nltk_data /opt/hf_cache \
    && python3 -m nltk.downloader -d /opt/nltk_data vader_lexicon \
    && python3 -c "from transformers import pipeline; pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')" \
    && chmod -R 777 /opt/nltk_data /opt/hf_cache

# PostgreSQL JDBC 드라이버 빌드 시점 다운로드 (실행 시 Maven 네트워크 다운로드 제거)
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN python3 -c "import urllib.request; urllib.request.urlretrieve('https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar', '/opt/spark/jars/postgresql.jar')" \
    && chmod 644 /opt/spark/jars/postgresql.jar \
    && mkdir -p /opt/spark/app /opt/spark/data /tmp/spark-local \
    && chown -R spark:spark /tmp/spark-local

# 소스 코드 및 설정 동기화
COPY jobs/ /opt/spark/app/
COPY scripts/ /opt/spark/scripts/
COPY config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/spark/work-dir
