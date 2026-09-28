"""Environment-based configuration for the session lake loader."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoaderSettings(BaseSettings):
    """Runtime settings loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # analysis_db 접속. 이름은 분석 서비스와 같은 변수를 쓴다.
    database_host: str = "localhost"
    database_port: int = Field(default=5432, ge=1, le=65535)
    database_name: str = "analysis_db"
    database_user: str = "nilm_admin"
    database_password: str = "change-me-local"

    # HDFS(WebHDFS) 접속과 저장 경로
    hdfs_url: str = "http://namenode:9870"
    hdfs_user: str = "root"
    session_bronze_base: str = "/nilm/bronze/appliance-session"
    session_manifest_base: str = "/nilm/manifests/job=session-lake-loader"
    receipt_bronze_base: str = "/nilm/bronze/analysis-processing-receipt"
    receipt_manifest_base: str = "/nilm/manifests/job=analysis-receipt-lake-loader"
    # 비어 있지 않으면 HDFS 대신 로컬 디렉터리를 레이크로 사용한다(개발·테스트용).
    lake_local_root: str = ""

    # 실행 주기·배치 크기·재시도
    loader_poll_seconds: float = Field(default=60, gt=0, le=3600)
    loader_batch_max_events: int = Field(default=5000, ge=1)
    loader_max_batches_per_cycle: int = Field(default=20, ge=1)
    loader_rows_per_file: int = Field(default=200_000, ge=1)
    loader_retry_backoff_seconds: float = Field(default=30, ge=0)
    loader_retry_max_backoff_seconds: float = Field(default=900, ge=0)
    # 0이면 제한 없이 재시도한다. 초과한 배치는 FAILED로 남고 status에 표시된다.
    loader_max_attempts: int = Field(default=0, ge=0)
    # 초기 적재 Parquet를 업로드 전에 잠시 두는 로컬 디렉터리. 비어 있으면 임시 폴더.
    loader_spool_dir: str = ""

    http_host: str = "0.0.0.0"
    http_port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = "INFO"


@lru_cache
def get_settings() -> LoaderSettings:
    return LoaderSettings()
