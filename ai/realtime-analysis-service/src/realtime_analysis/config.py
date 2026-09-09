"""Environment-based service configuration."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Kafka 주소나 임계치를 환경변수로 받아옴
class Settings(BaseSettings):
    """Runtime settings loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_input_topic: str = "power.raw.v1"
    kafka_dlq_topic: str = "dlq.analysis"
    kafka_analysis_event_topic: str = "analysis.event.v1"
    kafka_group_id: str = "realtime-analysis-service-v1"
    kafka_auto_offset_reset: str = "earliest"

    model_window_size: int = Field(default=299, ge=1)
    fake_on_appliances: str = ""
    baseline_file: str = "config/baselines.json"
    analysis_score_threshold: int = Field(default=80, ge=0, le=100)
    analysis_timezone: str = "Asia/Seoul"
    log_level: str = "INFO"

    @property
    def fake_on_appliance_types(self) -> tuple[str, ...]:
        return tuple(
            item.strip().upper()
            for item in self.fake_on_appliances.split(",")
            if item.strip()
        )

    def consumer_config(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.kafka_group_id,
            "auto.offset.reset": self.kafka_auto_offset_reset,
            "enable.auto.commit": False,
        }

    def producer_config(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "enable.idempotence": True,
            "message.timeout.ms": 20000,
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()
