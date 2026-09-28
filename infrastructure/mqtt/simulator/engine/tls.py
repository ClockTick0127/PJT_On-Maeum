"""
NILM 스마트홈 전력 시뮬레이터 MQTT TLS 암호화 연결 및 설정 모듈

CLI 및 웹 시뮬레이터가 공통으로 사용하는 TLS 설정 생성, 포트 결정 우선순위 해석,
인증서 및 Hostname/SAN 검증 강제, 환경변수 파싱 및 비밀번호 마스킹을 전담합니다.
"""

import os
import ssl
from typing import Optional, Union


def parse_tls_enabled(val: object, default: bool = False) -> bool:
    """
    MQTT_TLS_ENABLED 설정값을 불리언으로 파싱합니다.
    - None 또는 빈 문자열: default 반환 (기본값: False)
    - bool: 해당 값 직접 반환
    - 1 / 0: True / False 반환
    - true/false, yes/no, on/off (대소문자 무관) 지원
    - 인식할 수 없는 값인 경우 평문으로 다운그레이드되지 않도록 ValueError 발생
    """
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        if val == 1:
            return True
        if val == 0:
            return False
        raise ValueError(
            f"올바르지 않은 MQTT_TLS_ENABLED 정수 값입니다: {val}. 허용값: 1 또는 0"
        )

    cleaned = str(val).strip().lower()
    if not cleaned:
        return default

    if cleaned in ("true", "1", "yes", "y", "on"):
        return True
    if cleaned in ("false", "0", "no", "n", "off"):
        return False

    raise ValueError(
        f"올바르지 않은 MQTT_TLS_ENABLED 설정값입니다: '{val}'. "
        f"허용값: true/false, 1/0, yes/no, on/off"
    )


def resolve_mqtt_port(
    explicit_port: Optional[int] = None,
    tls_enabled: bool = False
) -> int:
    """
    MQTT 브로커 포트 번호를 다음 우선순위로 결정합니다:
    1. CLI 또는 호출부에서 명시한 포트 (explicit_port is not None)
    2. MQTT_PORT 환경변수 (설정되어 있고 비어있지 않은 경우)
    3. TLS 활성화 시 기본값 8883
    4. 평문 연결 시 기본값 1883
    """
    if explicit_port is not None:
        return int(explicit_port)

    env_port = os.getenv("MQTT_PORT")
    if env_port is not None and env_port.strip():
        try:
            return int(env_port.strip())
        except ValueError as err:
            raise ValueError(f"유효하지 않은 MQTT_PORT 환경변수 값입니다: '{env_port}'") from err

    return 8883 if tls_enabled else 1883


def mask_password(password: Optional[str]) -> str:
    """비밀번호 노출 방지를 위한 마스킹 문자열 반환"""
    return "***" if password else ""


def resolve_mqtt_config(
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    tls_enabled: Optional[bool] = None,
    ca_file: Optional[Union[str, os.PathLike]] = None,
) -> dict:
    """
    MQTT 연결 파라미터를 환경변수와 결합하여 단일 dict로 통합 해석합니다.
    """
    # 1. TLS 활성화 여부 해석
    if tls_enabled is not None:
        resolved_tls = bool(tls_enabled)
    else:
        resolved_tls = parse_tls_enabled(os.getenv("MQTT_TLS_ENABLED", "false"))

    # 2. 포트 번호 해석 (우선순위: 명시적 인자 > MQTT_PORT > TLS 8883 / 평문 1883)
    resolved_port = resolve_mqtt_port(explicit_port=port, tls_enabled=resolved_tls)

    # 3. 호스트, 계정, 비밀번호, CA 파일 해석
    resolved_host = host if (host is not None and str(host).strip()) else os.getenv("MQTT_HOST", "localhost")
    resolved_user = username if (username is not None and str(username).strip()) else os.getenv("MQTT_USER", "simulator_user")
    resolved_pass = password if password is not None else os.getenv("MQTT_PASS", "test1234")
    resolved_ca = ca_file if ca_file is not None else os.getenv("MQTT_CA_FILE", None)

    return {
        "host": resolved_host,
        "port": resolved_port,
        "username": resolved_user,
        "password": resolved_pass,
        "tls_enabled": resolved_tls,
        "ca_file": resolved_ca,
    }


def create_mqtt_tls_context(ca_file: Optional[Union[str, os.PathLike]]) -> ssl.SSLContext:
    """
    MQTT 브로커 TLS 연결을 위한 SSLContext를 생성하고 서버 인증서 및 SAN 검증을 강제합니다.

    - ca_file이 누락되거나 비어있는 경우: ValueError
    - ca_file이 존재하지 않는 경우: FileNotFoundError
    - ca_file 경로가 일반 파일이 아닌 경우: ValueError
    - ca_file을 읽을 수 없는 경우: PermissionError / OSError
    - ca_file이 손상되었거나 유효하지 않은 PEM 형식인 경우: ssl.SSLError
    """
    if ca_file is None:
        raise ValueError(
            "MQTT TLS가 활성화되었으나 CA 인증서 파일 경로가 지정되지 않았습니다. "
            "(환경변수: MQTT_CA_FILE 또는 CLI: --ca-file)"
        )

    ca_str = str(ca_file).strip()
    if not ca_str:
        raise ValueError(
            "MQTT CA 인증서 파일 경로가 비어 있습니다. "
            "(환경변수: MQTT_CA_FILE 또는 CLI: --ca-file)"
        )

    ca_path = os.path.abspath(ca_str)
    if not os.path.exists(ca_path):
        raise FileNotFoundError(f"MQTT CA 인증서 파일을 찾을 수 없습니다: '{ca_str}'")

    if not os.path.isfile(ca_path):
        raise ValueError(f"MQTT CA 인증서 경로가 일반 파일이 아닙니다: '{ca_str}'")

    # 실제 파일 읽기 시도 (권한 및 I/O 오류 사전 감지)
    try:
        with open(ca_path, "rb") as f:
            f.read(1)
    except PermissionError as err:
        raise PermissionError(f"MQTT CA 인증서 파일에 대한 읽기 권한이 없습니다 ({ca_str}): {err}") from err
    except OSError as err:
        raise OSError(f"MQTT CA 인증서 파일을 열 수 없습니다 ({ca_str}): {err}") from err

    # 실제 SSLContext 생성 및 CA 파일 로드
    try:
        context = ssl.create_default_context(cafile=ca_path)
    except ssl.SSLError as err:
        raise ssl.SSLError(f"MQTT CA 인증서 PEM 로드 실패 ({ca_str}): {err}") from err
    except Exception as err:
        raise ssl.SSLError(f"MQTT CA 인증서 로드 중 오류 발생 ({ca_str}): {err}") from err

    # 보안 강제: 서버 인증서 및 Hostname/SAN 검증 활성화 (검증 우회 불가)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    return context


def get_mqtt_tls_context(
    tls_enabled: bool = False,
    ca_file: Optional[Union[str, os.PathLike]] = None
) -> Optional[ssl.SSLContext]:
    """
    TLS 활성화 여부에 따라 SSLContext 또는 None을 반환합니다.
    - tls_enabled=False: None 반환 (평문 연결)
    - tls_enabled=True: create_mqtt_tls_context(ca_file) 호출
    """
    if not tls_enabled:
        return None
    return create_mqtt_tls_context(ca_file)
