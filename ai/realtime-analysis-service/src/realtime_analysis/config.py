"""Environment-based service configuration."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator, model_validator
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
    kafka_analysis_activity_topic: str = "analysis.activity.v1"
    kafka_analysis_data_quality_topic: str = "analysis.data-quality.v1"
    kafka_analysis_snapshot_topic: str = "analysis.snapshot.v1"
    kafka_group_id: str = "realtime-analysis-service-v1"
    kafka_auto_offset_reset: str = "earliest"
    kafka_partition_assignment_strategy: Literal["cooperative-sticky"] = (
        "cooperative-sticky"
    )
    kafka_group_instance_id: str | None = None
    kafka_outing_event_topic: str = "monitoring.household-presence.v1"
    kafka_outing_group_id: str = "realtime-analysis-service-outing-v1"
    kafka_outing_auto_offset_reset: str = "earliest"
    kafka_outing_group_instance_id: str | None = None

    database_host: str = "localhost"
    database_port: int = Field(default=5432, ge=1, le=65535)
    database_name: str = "analysis_db"
    database_user: str = "nilm_admin"
    database_password: str = "change-me-local"

    model_window_size: int = Field(default=299, ge=1)
    model_backend: Literal["fake", "real", "selected_scene"] = "fake"
    model_asset_root: str = ""
    model_household_id: str | None = Field(default=None, min_length=1, max_length=50)
    scene_events_enabled: bool = False
    scene_risk_threshold_seconds: int | None = Field(default=None, ge=1)
    scene_test_household_id: str | None = None
    scene_policy_id: str = Field(default='selected-test-v1', min_length=1, max_length=100)
    model_appliance: str = "kettle"
    model_device: str = "cpu"
    model_dtype: Literal["float32", "bfloat16"] = "float32"
    model_threads: int = Field(default=1, ge=1)
    kafka_scene_snapshot_topic: str = "analysis.scene.v2"
    model_manifest_file: str = "config/model_manifest.json"
    fake_on_appliances: str = ""
    baseline_file: str = "config/baselines.json"
    analysis_policy_file: str = "config/analysis_policies.json"
    analysis_timezone: str = "Asia/Seoul"
    analysis_run_id: str = Field(default="realtime-v1", min_length=1, max_length=100)
    analysis_pipeline_version: str = Field(default="1", min_length=1, max_length=50)
    analysis_expected_samples_per_day: int = Field(default=86_400, ge=1)
    analysis_observation_valid_coverage_ratio: float = Field(
        default=0.95,
        ge=0,
        le=1,
    )
    activity_index_publish_hour: int = Field(default=0, ge=0, le=23)
    activity_index_publish_minute: int = Field(default=10, ge=0, le=59)
    activity_index_scheduler_poll_seconds: float = Field(
        default=30,
        gt=0,
        le=300,
    )
    routine_baseline_window_days: int = Field(default=28, ge=1, le=365)
    routine_baseline_minimum_sample_days: int = Field(default=14, ge=1, le=365)
    routine_baseline_minimum_weekday_sample_days: int = Field(
        default=4,
        ge=1,
        le=52,
    )
    routine_baseline_minimum_daily_use_probability: float = Field(
        default=0.70,
        ge=0,
        le=1,
    )
    routine_baseline_refresh_seconds: float = Field(
        default=60,
        gt=0,
        le=3600,
    )
    analysis_data_gap_threshold_seconds: float = Field(default=120, gt=0)
    analysis_data_quality_poll_seconds: float = Field(
        default=5,
        gt=0,
        le=300,
    )
    analysis_data_recovery_confirmation_samples: int = Field(
        default=3,
        ge=1,
    )
    appliance_on_confirmation_samples: int = Field(default=3, ge=1)
    appliance_off_confirmation_samples: int = Field(default=3, ge=1)
    appliance_off_threshold_margin: float = Field(default=0.05, ge=0, le=1)
    http_host: str = "0.0.0.0"
    http_port: int = Field(default=8000, ge=1, le=65535)
    readiness_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    consumer_lag_refresh_seconds: float = Field(default=5.0, gt=0, le=300)
    analysis_e2e_clock_skew_tolerance_seconds: float = Field(
        default=0.1,
        ge=0,
        le=60,
    )
    log_level: str = "INFO"

    hdfs_url: str = "http://namenode:9870"
    bronze_base: str = "/nilm/bronze/power"
    bronze_manifest_base: str = "/nilm/manifests/job=bronze-loader"
    retention_manifest_base: str = "/nilm/manifests/job=retention"
    retention_enabled: bool = True
    retention_apply: bool = False
    retention_require_compaction: bool = True
    bronze_retention_days: int = Field(default=14, ge=1, le=3650)
    bronze_grace_days: int = Field(default=3, ge=0, le=365)
    retention_max_delete_bytes: int = Field(
        default=5 * 1024 * 1024 * 1024,
        ge=1,
    )
    retention_max_delete_dates: int = Field(default=1, ge=1, le=31)

    @field_validator(
        "kafka_group_instance_id",
        "kafka_outing_group_instance_id",
        "model_household_id",
        "scene_test_household_id",
        "scene_risk_threshold_seconds",
        mode="before",
    )
    @classmethod
    def blank_optional_string_is_disabled(cls, value: object) -> object:
        """Treat blank optional environment values as disabled."""

        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @model_validator(mode="after")
    def selected_scene_requires_isolated_configuration(self) -> "Settings":
        if self.model_backend in {"real", "selected_scene"} and not self.model_asset_root:
            raise ValueError(
                "Real model backends require MODEL_ASSET_ROOT"
            )
        if self.model_backend == "real" and self.model_window_size != 255:
            raise ValueError(
                "Real model backend requires MODEL_WINDOW_SIZE=255"
            )
        if self.scene_risk_threshold_seconds is not None:
            if not self.scene_events_enabled or not self.scene_test_household_id or self.scene_test_household_id != self.model_household_id:
                raise ValueError('Scene risk requires enabled events and an explicit matching test household')
        if self.model_backend == "selected_scene":
            if self.analysis_run_id == "realtime-v1":
                raise ValueError("Selected scene requires a dedicated ANALYSIS_RUN_ID")
            if self.kafka_input_topic == "power.raw.v1" or self.kafka_group_id == "realtime-analysis-service-v1":
                raise ValueError("Selected scene requires dedicated input topic and consumer group")
            if self.kafka_scene_snapshot_topic == self.kafka_analysis_snapshot_topic:
                raise ValueError("Selected scene output must not use the legacy snapshot topic")
        return self

    @model_validator(mode="after")
    def baseline_sample_days_must_fit_window(self) -> "Settings":
        if (
            self.routine_baseline_minimum_sample_days
            > self.routine_baseline_window_days
        ):
            raise ValueError(
                "ROUTINE_BASELINE_MINIMUM_SAMPLE_DAYS must not exceed "
                "ROUTINE_BASELINE_WINDOW_DAYS"
            )
        return self

    @property
    def fake_on_appliance_types(self) -> tuple[str, ...]:
        return tuple(
            item.strip().upper()
            for item in self.fake_on_appliances.split(",")
            if item.strip()
        )

    def consumer_config(self) -> dict[str, object]:
        config: dict[str, object] = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.kafka_group_id,
            "auto.offset.reset": self.kafka_auto_offset_reset,
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "partition.assignment.strategy": (
                self.kafka_partition_assignment_strategy
            ),
        }
        if self.kafka_group_instance_id is not None:
            config["group.instance.id"] = self.kafka_group_instance_id
        return config

    def producer_config(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "enable.idempotence": True,
            "message.timeout.ms": 20000,
        }

    def outing_consumer_config(self) -> dict[str, object]:
        """Kafka options reserved for the monitoring outing event consumer."""

        config: dict[str, object] = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.kafka_outing_group_id,
            "auto.offset.reset": self.kafka_outing_auto_offset_reset,
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "partition.assignment.strategy": (
                self.kafka_partition_assignment_strategy
            ),
        }
        if self.kafka_outing_group_instance_id is not None:
            config["group.instance.id"] = self.kafka_outing_group_instance_id
        return config


@lru_cache
def get_settings() -> Settings:
    return Settings()
