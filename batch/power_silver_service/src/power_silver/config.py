"""Environment-based configuration for the power-silver-daily Spark batch."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_TARGETS_FILE = str(Path(__file__).resolve().parent / "config" / "observation_targets.json")


class SilverSettings(BaseSettings):
    """Runtime settings loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # analysis_db 접속. 이름은 분석·집계 서비스와 같은 변수를 쓴다.
    database_host: str = "localhost"
    database_port: int = Field(default=5432, ge=1, le=65535)
    database_name: str = "analysis_db"
    database_user: str = "nilm_admin"
    database_password: str = "change-me-local"

    # 레이크 접근. 목록·manifest는 WebHDFS로, 데이터 읽기·쓰기는 Spark가 fs URI로 한다.
    hdfs_url: str = "http://namenode:9870"
    hdfs_user: str = "root"
    lake_fs_uri: str = "hdfs://namenode:9000"
    # 비어 있지 않으면 HDFS 대신 로컬 디렉터리를 레이크로 사용한다(개발·테스트용).
    lake_local_root: str = ""

    # 입력·출력 경로
    bronze_base: str = "/nilm/bronze/power"
    bronze_manifest_base: str = "/nilm/manifests/job=bronze-loader"
    silver_base: str = "/nilm/silver"
    quarantine_base: str = "/nilm/quarantine/power_silver"
    manifest_base: str = "/nilm/manifests/job=power-silver-daily"
    staging_base: str = "/nilm/silver/.staging/job=power-silver-daily"
    session_manifest_base: str = "/nilm/manifests/job=session-lake-loader"
    receipt_manifest_base: str = "/nilm/manifests/job=analysis-receipt-lake-loader"
    analysis_manifest_base: str = "/nilm/manifests/job=analysis-usage-daily"
    analysis_staging_base: str = "/nilm/silver/.staging/job=analysis-usage-daily"
    gold_base: str = "/nilm/gold"
    analysis_rule_version: str = "analysis-coverage-v1"
    analysis_run_id: str = Field(default="realtime-v1", min_length=1, max_length=100)
    quality_policy_version: str = "baseline-quality-v1"
    analysis_minimum_coverage_ratio: float = Field(default=0.95, ge=0, le=1)
    analysis_maximum_gap_seconds: int = Field(default=120, ge=0)

    # 업무 날짜 기준. 한국은 서머타임이 없어 고정 오프셋이 tz 데이터베이스보다 안전하다.
    business_utc_offset_seconds: int = Field(default=9 * 3600, gt=-86400, lt=86400)

    # 관측 대상 가구·계측기 스냅샷
    observation_targets_file: str = DEFAULT_TARGETS_FILE

    # 정제·관측 판정 규칙
    rule_version: str = "power-silver-v1"
    valid_coverage_ratio: float = Field(default=0.95, gt=0, le=1)
    # 측정시각이 이 시간 이상 미래면 격리한다.
    future_skew_seconds: int = Field(default=300, ge=0)

    # 입력 준비 판정
    # 대상일 종료 후 이 시간이 지나기 전에는, 적재기가 날짜 경계를 넘겼다는 증거가 없으면 기다린다.
    input_ready_grace_seconds: int = Field(default=1800, ge=0)
    # 이 시간이 지나면 남은 파티션을 기다리지 않고 있는 입력으로 확정한다.
    input_wait_deadline_seconds: int = Field(default=6 * 3600, ge=0)
    # 대상일보다 앞선 수집일 폴더도 이 일수만큼 훑는다(생산자 시계가 빠른 경우).
    manifest_scan_back_days: int = Field(default=1, ge=0, le=31)

    # 출력 파일 크기 조절
    power_clean_target_file_bytes: int = Field(default=192 * 1024 * 1024, ge=1024 * 1024)
    # 이전 실행에서 측정한 값이 없을 때 쓰는 초기 추정치(압축 후 행당 바이트).
    power_clean_default_bytes_per_row: float = Field(default=32.0, gt=0)
    power_clean_max_output_files: int = Field(default=2000, ge=1)

    # Spark
    spark_master: str = ""
    spark_app_name: str = "power-silver-daily"
    spark_shuffle_partitions: int = Field(default=0, ge=0)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> SilverSettings:
    return SilverSettings()
