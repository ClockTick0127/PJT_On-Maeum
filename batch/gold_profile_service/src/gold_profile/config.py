from functools import lru_cache

from pydantic import Field

from power_silver.config import SilverSettings


class GoldProfileSettings(SilverSettings):
    profile_window_days: int = Field(default=28, ge=1, le=365)
    profile_minimum_sample_days: int = Field(default=14, ge=1, le=365)
    profile_minimum_weekday_sample_days: int = Field(default=4, ge=1, le=52)
    profile_minimum_daily_use_probability: float = Field(default=0.70, ge=0, le=1)
    profile_time_bucket_minutes: int = Field(default=30, ge=5, le=1440)
    profile_rule_version: str = "gold-profile-v1"
    profile_statistic_rule_version: str = "household-statistics-v1-nearest-rank"
    profile_manifest_base: str = "/nilm/manifests/job=gold-profile"
    profile_staging_base: str = "/nilm/gold/.staging/job=gold-profile"
    # This is included in Gold's config_version.  Changing SHADOW -> ACTIVE
    # therefore creates a new run/version instead of relabelling old history.
    profile_delivery_mode: str = Field(default="SHADOW", pattern="^(ACTIVE|SHADOW)$")
    # 아웃박스를 비우는 발행자가 붙을 브로커. 이름은 분석·집계 서비스와 같은 변수를 쓴다.
    kafka_bootstrap_servers: str = "localhost:9092"
    profile_kafka_topic: str = "gold.household-profile.v1"
    profile_publisher_batch_size: int = Field(default=100, ge=1, le=10_000)
    profile_publisher_retry_seconds: int = Field(default=30, ge=1, le=86_400)
    spark_app_name: str = "gold-profile"


@lru_cache
def get_settings() -> GoldProfileSettings:
    return GoldProfileSettings()

