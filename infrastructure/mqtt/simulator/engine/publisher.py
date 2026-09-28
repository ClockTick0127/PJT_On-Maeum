"""
NILM 스마트홈 전력 시뮬레이터 MQTT 비동기 발행 모듈
"""

import json
import uuid
import aiomqtt

from .power_model import calculate_main_panel_metrics


async def publish_house_power(
    client: aiomqtt.Client,
    house: str,
    now_iso: str,
    qos: int = 1,
    allow_random: bool = True,
    metrics: dict | None = None
) -> dict:
    """단일 가구의 메인 분전반 전력 계측 데이터(4특징 및 물리 특성) 발행"""
    if metrics is None:
        metrics = calculate_main_panel_metrics(house, allow_random=allow_random)

    payload = {
        # [신규 명세] realtime-analysis-service MVP 요구사항 명세서 6.1 규격 대응
        "message_id": str(uuid.uuid4()),
        "household_id": house,
        "device_id": "main",
        "measured_at": now_iso,
        "active_power": metrics["active_power"],     # AI 모델 입력 피처 1 (W)
        "reactive_power": metrics["reactive_power"], # AI 모델 입력 피처 2 (var)
        "power_factor": metrics["power_factor"],     # AI 모델 입력 피처 3 (역률)
        "current": metrics["current"],               # AI 모델 입력 피처 4 (A)

        # [하위 호환] 기존 MQTT-Kafka Bridge, HDFS Loader 및 레거시 호환 필드
        "house": house,
        "device": "main",
        "ts": now_iso,
        "power_w": metrics["active_power"],
        "voltage": metrics["voltage"],               # 전압 (V)
        "apparent_power": metrics["apparent_power"]  # 피상전력 (VA)
    }

    topic = f"v1/power/sim/{house}/main"
    await client.publish(topic, json.dumps(payload), qos=qos)
    return {
        "house": house,
        "power": metrics["active_power"],
        "devices": metrics["active_devices"],
        "metrics": metrics,
    }
