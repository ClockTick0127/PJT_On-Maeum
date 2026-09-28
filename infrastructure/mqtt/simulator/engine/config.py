"""
NILM 스마트홈 전력 시뮬레이터 브로커 및 기본 가구 설정
"""

# ==========================================
# 1. 브로커 접속 및 시뮬레이션 기본 설정
# ==========================================
# 설정 해석 지연: import 시점 환경변수 파싱 오류를 방지하고,
# CLI 명시 인자 > 환경변수 > 기본값 우선순위가 항상 보장되도록 정적 기본 상수를 정의합니다.
DEFAULT_TLS_ENABLED = False
DEFAULT_BROKER_HOST = "localhost"
DEFAULT_BROKER_PORT = 1883
DEFAULT_BROKER_USER = "simulator_user"
DEFAULT_BROKER_PASS = "test1234"
DEFAULT_CA_FILE = None

# 시뮬레이션 기본 대상 가구 목록 (H001 ~ H010 총 10개 가구)
DEFAULT_HOUSES = [f"H{i:03d}" for i in range(1, 11)]
