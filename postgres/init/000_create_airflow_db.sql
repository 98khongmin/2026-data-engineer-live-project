-- Airflow 메타데이터 저장을 위한 전용 데이터베이스 생성
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;

