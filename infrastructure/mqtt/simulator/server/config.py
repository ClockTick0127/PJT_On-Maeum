"""
NILM 스마트홈 전력 시뮬레이터 웹 서버 설정 모듈

웹 서버에서 사용하는 기본 포트 및 허용 시나리오 목록 등 고정 설정을 정의합니다.
MQTT 브로커 관련 설정은 simulator.py의 환경변수 기반 설정을 그대로 사용합니다.
"""

DEFAULT_PORT = 8085
ALLOWED_SCENARIOS = {"peak", "routine_missed", "routine_missed_demo", "random", "manual", "normal_routine", "prolonged_use", "sensor_fault"}
MIN_INTERVAL = 0.1
MAX_INTERVAL = 10.0
DEFAULT_INTERVAL = 1.0

# ==========================================
# 2. E2E ACCELERATED 자동 배속
# ==========================================
# execution.speed를 생략한 ACCELERATED 실행에 적용할 기본 발행 속도(초당 메시지, 전 가구 합계).
#
# MQTT -> 브리지 -> Kafka 경로는 브리지가 Kafka 확인 후에야 MQTT를 ack하므로 발행자까지
# 이어지는 백프레셔 경로가 없다. 브로커의 브리지용 송신 큐가 유일한 완충 장치이며,
# 이를 넘기는 순간 브로커가 조용히 버린다(브리지는 에러를 남기지 않는다).
#
# 2026-09-20 로컬 스택 실측 (브로커 큐 설정 반영 후):
#   - 브리지 실효 처리량: 약 1,477/s (20일 1,728,000건 발행의 드레인 시간에서 역산)
#   - 브로커 큐 실효 깊이: 100,000건 (mosquitto.local.conf의 max_queued_messages)
#
# 발행 속도가 브리지 처리량을 넘으면 초과분이 큐에 누적되므로, 긴 실행일수록
# 누적량이 큐를 넘길 위험이 커진다. 이 상수는 브리지 처리량 아래로 잡아
# 큐 누적 자체가 일어나지 않게 하는 값이다(약 19% 여유).
#
# 이 한계는 가구당이 아니라 합계 기준이므로 가구 수로 나누어 적용한다.
DEFAULT_SAFE_AGGREGATE_RATE = 1200.0


def resolve_auto_speed(
    household_count: int,
    aggregate_rate: float = DEFAULT_SAFE_AGGREGATE_RATE,
) -> float:
    """
    가구 수에 따라 ACCELERATED 자동 배속(가구당 speed_multiplier)을 산출한다.

    합계 발행 속도가 aggregate_rate를 넘지 않도록 가구 수로 나눈 값을 반환한다.
    예: 1가구 -> 700.0, 2가구 -> 350.0, 10가구 -> 70.0
    """
    if isinstance(household_count, bool) or not isinstance(household_count, int):
        raise ValueError(f"household_count는 정수여야 합니다: {household_count!r}")
    if household_count < 1:
        raise ValueError(f"household_count는 1 이상이어야 합니다: {household_count}")
    if isinstance(aggregate_rate, bool) or not isinstance(aggregate_rate, (int, float)):
        raise ValueError(f"aggregate_rate는 숫자여야 합니다: {aggregate_rate!r}")
    import math as _math
    if not _math.isfinite(aggregate_rate) or aggregate_rate <= 0:
        raise ValueError(f"aggregate_rate는 0보다 큰 유한한 숫자여야 합니다: {aggregate_rate}")
    return float(aggregate_rate) / household_count


def validate_interval(val) -> float:
    """
    발행 주기(interval) 유효성 공통 검증 함수.

    허용 범위: 0.1초 이상 10.0초 이하
    거절 대상:
    - None / null
    - bool (Python에서 bool은 int의 서브클래스이므로 isinstance(val, bool) 검사 필요)
    - str 및 비숫자 타입
    - 0, 음수
    - NaN, Infinity 등 비유한 숫자
    - 0.1 미만 또는 10.0 초과
    """
    if val is None:
        raise ValueError("interval 필드는 필수입니다.")
    if isinstance(val, bool):
        raise ValueError("interval은 boolean 타입일 수 없습니다.")
    if not isinstance(val, (int, float)):
        raise ValueError(f"interval은 숫자(int 또는 float)여야 합니다. (전달된 타입: {type(val).__name__})")

    val_float = float(val)
    import math
    if not math.isfinite(val_float):
        raise ValueError("interval은 유한한(finite) 실수여야 합니다.")
    if val_float < MIN_INTERVAL or val_float > MAX_INTERVAL:
        raise ValueError(f"interval은 {MIN_INTERVAL}초 이상 {MAX_INTERVAL}초 이하의 값이어야 합니다. (전달된 값: {val_float})")

    return val_float
