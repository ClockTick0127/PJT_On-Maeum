"""
NILM 스마트홈 전력 시뮬레이터 (호환 Facade 및 CLI 실행 진입점)
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta
import asyncio
import argparse
import aiomqtt

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

from scenarios import (
    KST,
    RoutineMissedScenario,
    NormalRoutineScenario,
    SensorFaultScenario,
    parse_simulation_date,
    resolve_simulation_start_time,
    format_iso_utc,
    parse_simulation_start_time,
)

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Windows 환경 호환성 설정 (aiomqtt 소켓 처리를 위해 SelectorEventLoop 적용)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ==========================================
# engine 패키지 하위 호환 Facade Re-export
# ==========================================
from engine.config import (
    DEFAULT_BROKER_HOST,
    DEFAULT_BROKER_PORT,
    DEFAULT_BROKER_USER,
    DEFAULT_BROKER_PASS,
    DEFAULT_TLS_ENABLED,
    DEFAULT_CA_FILE,
    DEFAULT_HOUSES,
)
from engine.tls import (
    resolve_mqtt_port,
    get_mqtt_tls_context,
    resolve_mqtt_config,
)
from engine.profiles import DEVICE_PROFILES
from engine.state import (
    house_states,
    device_states,
    house_rngs,
    init_simulation_states,
    set_manual_device_state,
    get_house_rng,
    resolve_seed,
)
from engine.power_model import (
    inject_peak_scenario_event,
    inject_normal_routine_scenario_event,
    inject_prolonged_use_scenario_event,
    update_house_environment,
    update_and_generate_device_load,
    calculate_main_panel_metrics,
)
from engine.publisher import publish_house_power

# 기본 10가구로 초기 상태 셋업 (하위 호환성 유지)
init_simulation_states(DEFAULT_HOUSES)


def parse_args(args=None):
    """커맨드라인 실행 인자 파싱"""
    parser = argparse.ArgumentParser(
        description="NILM IoT 스마트홈 메인 분전반 전력 시뮬레이터 (MQTT 비동기 발행)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=["random", "peak", "routine_missed", "normal_routine", "sensor_fault"],
        default="random",
        help="시뮬레이션 시나리오 모드 (random: 확률 기반 연속 시뮬레이션, peak: H001 단일 가구 10초 3,000W+ 피크 시연 모드, routine_missed: H001 08:10 루틴 누락 이상치 검증 모드, normal_routine: H001 08:10 이전 정상 아침 루틴 시연 모드, sensor_fault: 가변 센서 고장(결측) 및 복구 시연 모드)"
    )
    parser.add_argument(
        "--houses", "-n",
        type=int,
        default=10,
        help="시뮬레이션 대상 가구 수 (H001~H{n:03d} 자동 생성)"
    )
    parser.add_argument(
        "--fault-duration-sec",
        type=int,
        default=120,
        help="센서 결측(고장) 지속 시간 (초, 1~3600, 기본값: 120)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="난수 seed (0~2147483647). 같은 seed + 같은 시나리오/가구면 계측값이 그대로 재현됨. 미지정 시 자동 생성 후 출력"
    )
    parser.add_argument(
        "--interval", "-i",
        type=float,
        default=1.0,
        help="데이터 발행 주기 (초 단위)"
    )
    parser.add_argument(
        "--hz",
        type=float,
        default=None,
        help="가구당 초당 측정 횟수 (지정 시 interval = 1/hz 로 자동 환산)"
    )
    parser.add_argument(
        "--count", "-c",
        type=int,
        default=0,
        help="전송 사이클 횟수 (0: 무한 연속 발행, N > 0: N회 전송 후 자동 종료, peak 기본값: 60회, routine_missed 기본값: 300회, sensor_fault: 10+D+10회)"
    )
    parser.add_argument(
        "--date", "-d",
        default=None,
        help="시뮬레이션 데이터 기준 날짜 (YYYY-MM-DD 형식, 예: '2026-09-10')"
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help="시뮬레이션 시작 가상 시각 (예: '08:15:00' 또는 '2026-09-08T08:15:00'). routine_missed 모드는 기본값으로 오늘 아침 08:15:00 KST 적용"
    )
    parser.add_argument(
        "--host",
        default=None,
        help="MQTT 브로커 호스트 주소 (미지정 시 MQTT_HOST 환경변수 또는 localhost. TLS 시 인증서 SAN과 일치 필수)"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=None,
        help="MQTT 브로커 포트 번호 (미지정 시 MQTT_PORT 환경변수 또는 TLS 여부에 따라 8883/1883 자동 결정)"
    )
    parser.add_argument(
        "--user", "-u",
        default=None,
        help="MQTT 인증 사용자명 (미지정 시 MQTT_USER 환경변수 또는 simulator_user)"
    )
    parser.add_argument(
        "--password",
        default=None,
        help="MQTT 인증 비밀번호 (미지정 시 MQTT_PASS 환경변수 또는 test1234. 프로세스 노출 방지를 위해 환경변수/입력 권장)"
    )
    parser.add_argument(
        "--tls",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="MQTT TLS 암호화 연결 활성화 여부 (--tls 또는 --no-tls, 미지정 시 MQTT_TLS_ENABLED 환경변수)"
    )
    parser.add_argument(
        "--ca-file",
        default=None,
        help="MQTT TLS 브로커 인증서 검증에 사용할 CA 파일 경로 (PEM 형식, 미지정 시 MQTT_CA_FILE 환경변수)"
    )
    parser.add_argument(
        "--qos",
        type=int,
        default=1,
        choices=[0, 1],
        help="MQTT 발행 QoS 레벨"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="요약 모드 (매초 상세 로그 생략 및 5초 주기 통계/TPS 출력)"
    )
    parsed = parser.parse_args(args)

    # CLI 인자 > 환경변수 > TLS 기본값 우선순위로 설정 단일 해석
    cfg = resolve_mqtt_config(
        host=parsed.host,
        port=parsed.port,
        username=parsed.user,
        password=parsed.password,
        tls_enabled=parsed.tls,
        ca_file=parsed.ca_file,
    )
    parsed.host = cfg["host"]
    parsed.port = cfg["port"]
    parsed.user = cfg["username"]
    parsed.password = cfg["password"]
    parsed.tls = cfg["tls_enabled"]
    parsed.ca_file = cfg["ca_file"]
    return parsed


async def run_simulator(args):
    """시뮬레이터 메인 비동기 실행 루프"""
    is_peak_mode = (args.scenario == "peak")
    is_missed_mode = (args.scenario == "routine_missed")
    is_normal_mode = (args.scenario == "normal_routine")
    is_sensor_fault_mode = (args.scenario == "sensor_fault")

    # sensor_fault 모드 및 --count / fault_duration_sec 충돌 검증 (TLS 컨텍스트 생성 및 MQTT 연결 전 즉시 실행)
    fault_duration_sec = getattr(args, "fault_duration_sec", SensorFaultScenario.DEFAULT_FAULT_DURATION_SEC)
    if is_sensor_fault_mode:
        if type(fault_duration_sec) is not int or isinstance(fault_duration_sec, bool) or not (1 <= fault_duration_sec <= 3600):
            raise ValueError("fault_duration_sec는 1~3600 범위의 정수여야 합니다.")
        expected_total = fault_duration_sec + 20
        if args.count not in (0, expected_total):
            raise ValueError(
                f"sensor_fault 시나리오에서 --count({args.count})는 결측 시간({fault_duration_sec}초) 기준 총 사이클 수({expected_total})와 일치해야 합니다. (또는 0으로 기본값 사용)"
            )
        target_count = expected_total
    elif args.count > 0:
        target_count = args.count
    elif is_peak_mode:
        target_count = 60
    elif is_missed_mode:
        target_count = 300  # 299개 슬라이딩 버퍼 완충 후 이상 검증
    elif is_normal_mode:
        target_count = NormalRoutineScenario.TOTAL_CYCLES
    else:
        target_count = 0

    # TLS 컨텍스트 생성 및 사전 검증 (CA 파일 누락/오류 시 연결 전 즉각 실패)
    tls_context = get_mqtt_tls_context(tls_enabled=args.tls, ca_file=args.ca_file)
    tls_desc = f" [TLS ON | CA: {args.ca_file}]" if args.tls else " [TLS OFF (평문)]"

    # 1. 가구 목록 동적 생성 및 상태 머신 초기화
    if is_normal_mode:
        if args.houses > 1 and args.houses != 10:
            raise ValueError("normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")
        houses = ["H001"]
    elif (is_peak_mode or is_missed_mode or is_sensor_fault_mode) and args.houses == 10:
        houses = ["H001"]  # 단일 가구 H001 대상 시연 모드
    else:
        houses = [f"H{i:03d}" for i in range(1, args.houses + 1)]

    try:
        seed = resolve_seed(args.seed)
    except ValueError as err:
        print(f"[오류] {err}", file=sys.stderr)
        sys.exit(1)
    init_simulation_states(houses, seed=seed)

    # 2. 발행 주기(interval) 계산
    interval = (1.0 / args.hz) if args.hz and args.hz > 0 else max(0.001, args.interval)

    allow_random = not (is_peak_mode or is_missed_mode or is_normal_mode or is_sensor_fault_mode)
    try:
        sim_date_parsed = parse_simulation_date(args.date) if args.date else None
        base_dt = resolve_simulation_start_time(
            scenario=args.scenario,
            simulation_date=sim_date_parsed,
            start_time_str=args.start_time,
            routine_default_time="08:15:00"
        )
    except ValueError as err:
        print(f"[오류] 시작 일시 설정 오류: {err}", file=sys.stderr)
        sys.exit(1)

    print(f"============================================================")
    if is_peak_mode:
        start_desc = base_dt.strftime('%Y-%m-%d %H:%M:%S KST') if base_dt else '현재 시각 (실시간)'
        print(f" NILM IoT 전력 시뮬레이터 시작 [10초 3,000W+ 피크 시연 모드]")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos}){tls_desc}")
        print(f" - 대상 가구: {', '.join(houses)} (총 {len(houses)}개)")
        print(f" - 시작 가상 시각: {start_desc}")
        print(f" - 시연 타임라인:")
        print(f"   * T+01s ~ T+09s: 평상시 대기 상태 (약 55~65W)")
        print(f"   * T+10s ~ T+30s: [피크 경보] 전기포트(1,700W) + 인덕션(1,600W) 동시 기동 (3,300~3,500W 도달)")
        print(f"   * T+31s ~ T+44s: [피크 해소] 전기포트 자동 정지, 인덕션 단독 가동 (약 1,600W)")
        print(f"   * T+45s ~ T+60s: [정상 복귀] 인덕션 조리 완료, 대기전력 상태 복귀 (약 60W)")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        print(f" - 목표 사이클: {target_count}회 발행 후 자동 종료")
    elif is_missed_mode:
        start_desc = base_dt.strftime('%Y-%m-%d %H:%M:%S KST') if base_dt else '현재 시각'
        print(f" NILM IoT 전력 시뮬레이터 시작 [루틴 누락(ROUTINE_MISSED) 이상치 검증 모드]")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos}){tls_desc}")
        print(f" - 대상 가구: {', '.join(houses)} (총 {len(houses)}개)")
        print(f" - 시작 가상 시각: {start_desc}")
        print(f" - 시연 타임라인:")
        print(f"   * 전자레인지 가동 없이 대기전력(약 45~65W) 및 냉장고 주기만 연속 유지")
        print(f"   * T+001s ~ T+298s: 분석 서비스 입력 윈도우(299개) 슬라이딩 버퍼 적재")
        print(f"   * T+299s: 299개 분석 입력 데이터 충족 — AI 이상 감지 판정 대기")
        print(f"   * T+300s: 검증 완료 후 자동 종료 (H001 루틴 누락 전력 패턴 발행 완료)")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        print(f" - 목표 사이클: {target_count}회 발행 후 자동 종료")
    elif is_normal_mode:
        start_desc = base_dt.strftime('%Y-%m-%d %H:%M:%S KST') if base_dt else '현재 시각'
        print(f" NILM IoT 전력 시뮬레이터 시작 [정상 일상(NORMAL_ROUTINE) 아침 루틴 모드]")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos}){tls_desc}")
        print(f" - 대상 가구: {', '.join(houses)} (총 {len(houses)}개)")
        print(f" - 시작 가상 시각: {start_desc}")
        print(f" - 시연 타임라인:")
        print(f"   * T+001s ~ T+242s: 평상시 아침 대기 상태 (약 45~65W)")
        print(f"   * T+243s ~ T+302s: [아침 루틴 가동] 전자레인지(940W) 정확히 60초간 가동")
        print(f"   * T+303s ~ T+{NormalRoutineScenario.TOTAL_CYCLES:03d}s: [루틴 완료] 전자레인지 가동 종료 후 대기전력 복귀 및 완료")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        print(f" - 목표 사이클: {target_count}회 발행 후 자동 종료")
    elif is_sensor_fault_mode:
        tl = SensorFaultScenario.get_timeline(fault_duration_sec)
        start_desc = base_dt.strftime('%Y-%m-%d %H:%M:%S KST') if base_dt else '현재 시각 (실시간)'
        print(f" NILM IoT 전력 시뮬레이터 시작 [센서 고장(SENSOR_FAULT) {tl.duration_sec}초 결측 검증 모드]")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos}){tls_desc}")
        print(f" - 대상 가구: {', '.join(houses)} (총 {len(houses)}개)")
        print(f" - 시작 가상 시각: {start_desc}")
        print(f" - 결측 지속 시간: {tl.duration_sec}초")
        print(f" - 시연 타임라인:")
        print(f"   * T+001s ~ T+010s: 평상시 정상 대기전력 계측 및 MQTT 발행 (10회)")
        print(f"   * T+{tl.fault_start:03d}s ~ T+{tl.fault_end:03d}s: [센서 고장] {tl.duration_sec}초간 MQTT 메시지 0건 발행 (결측 구간)")
        print(f"   * T+{tl.recovery_start:03d}s ~ T+{tl.recovery_end:03d}s: [센서 복구] 계측 복구 및 정상 MQTT 발행 재개 (10회)")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        print(f" - 목표 사이클: {target_count}회 진행 후 자동 종료 (총 {len(houses) * 20}건 발행)")
    else:
        start_desc = base_dt.strftime('%Y-%m-%d %H:%M:%S KST') if base_dt else '현재 시각 (실시간)'
        print(f" NILM IoT 전력 시뮬레이터 시작")
        print(f" - 브로커: {args.host}:{args.port} (QoS {args.qos}){tls_desc}")
        print(f" - 대상 가구: 총 {len(houses)}개 ({houses[0]} ~ {houses[-1]})")
        print(f" - 시작 가상 시각: {start_desc}")
        print(f" - 전송 주기: {interval:.3f}초 (약 {1.0/interval:.1f}Hz)")
        if target_count > 0:
            print(f" - 목표 사이클: {target_count}회 발행 후 자동 종료 (총 {len(houses) * target_count}건)")
        else:
            print(f" - 실행 모드: 무한 연속 발행 (종료: Ctrl+C)")
    print(f" - Seed: {seed} (재현: --seed {seed})")
    print(f"============================================================", flush=True)

    # sensor_fault 모드에서 base_dt가 없으면 시작 시각을 1회 고정하여
    # 배속 실행 시에도 measured_at이 가상 1초 간격으로 증가하도록 보장
    if is_sensor_fault_mode and base_dt is None:
        base_dt = datetime.now(timezone.utc)

    total_sent = 0
    cycle = 0
    start_time = time.time()
    last_summary_time = start_time
    last_summary_sent = 0

    async with aiomqtt.Client(
        hostname=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        tls_context=tls_context,
        keepalive=60,
        timeout=5
    ) as client:
        # 대량 가구 동시 발행 시 aiomqtt 기본 경고 임계값(10) 조정
        client.pending_calls_threshold = max(len(houses) * 4, 200)

        while True:
            cycle_start = time.time()
            if base_dt is not None:
                sim_dt = base_dt + timedelta(seconds=cycle)
                now_iso = format_iso_utc(sim_dt)
            else:
                now_iso = format_iso_utc(datetime.now(timezone.utc))
            cycle += 1

            # 시연 모드별 타임라인 이벤트 주입
            event_desc = None
            if is_peak_mode:
                event_desc = inject_peak_scenario_event(cycle, "H001")
            elif is_normal_mode:
                event_desc = inject_normal_routine_scenario_event(cycle, "H001")

            # N개 가구 동시 비동기 발행 (Concurrent Publish)
            # sensor_fault 모드의 고장 구간에서는 MQTT 메시지를 발행하지 않음
            is_fault = is_sensor_fault_mode and SensorFaultScenario.is_fault_cycle(cycle, duration_sec=fault_duration_sec)
            if is_fault:
                for house in houses:
                    calculate_main_panel_metrics(house, allow_random=allow_random)
                results = []
            else:
                results = await asyncio.gather(
                    *(publish_house_power(client, house, now_iso, qos=args.qos, allow_random=allow_random) for house in houses)
                )
                total_sent += len(houses)

            # 로그 출력 제어
            if is_peak_mode:
                res = results[0]
                power_w = res["power"]
                devs = res["devices"]
                dev_str = ", ".join(devs) if devs else "대기(기저부하)"

                status_tag = "대기"
                if power_w >= 3000.0:
                    status_tag = "피크 경보 (3,000W+ 초과!)"
                elif power_w >= 1000.0:
                    status_tag = "가전 가동 중"

                event_notice = f"  <== [{event_desc}]" if event_desc else ""
                print(f"[{now_iso}] (T+{cycle:02d}s) 소비전력: {power_w:7.1f} W | 상태: {status_tag:<22} | 가전: {dev_str}{event_notice}", flush=True)
            elif is_missed_mode:
                res = results[0]
                power_w = res["power"]
                devs = res["devices"]
                dev_str = ", ".join(devs) if devs else "대기전력(기저부하)"
                sim_dt = base_dt + timedelta(seconds=cycle - 1) if base_dt else datetime.now(KST)
                kst_str = sim_dt.astimezone(KST).strftime("%H:%M:%S")

                status_tag, notice = RoutineMissedScenario.get_cycle_status(cycle)
                print(f"[{now_iso} | KST {kst_str}] (T+{cycle:03d}s) 소비전력: {power_w:6.1f} W | 상태: {status_tag:<28} | 가전: {dev_str}{notice}", flush=True)
            elif is_normal_mode:
                res = results[0]
                power_w = res["power"]
                devs = res["devices"]
                dev_str = ", ".join(devs) if devs else "대기전력(기저부하)"
                sim_dt = base_dt + timedelta(seconds=cycle - 1) if base_dt else datetime.now(KST)
                kst_str = sim_dt.astimezone(KST).strftime("%H:%M:%S")
                status_tag = "전자레인지 가동 중" if "전자레인지" in devs else "정상 대기"
                event_notice = f"  <== [{event_desc}]" if event_desc else ""
                print(f"[{now_iso} | KST {kst_str}] (T+{cycle:03d}s) 소비전력: {power_w:6.1f} W | 상태: {status_tag:<20} | 가전: {dev_str}{event_notice}", flush=True)
            elif is_sensor_fault_mode:
                status_tag, notice = SensorFaultScenario.get_cycle_status(cycle, duration_sec=fault_duration_sec)
                notice_str = f"  <== [{notice}]" if notice else ""
                if is_fault:
                    _, gap_elapsed, gap_remaining = SensorFaultScenario.get_gap_metrics(cycle, duration_sec=fault_duration_sec)
                    print(f"[{now_iso}] (T+{cycle:03d}s) [센서 고장 / 측정 없음] MQTT 미발행 | 상태: {status_tag:<22} | 센서 고장 ({gap_elapsed}/{fault_duration_sec}초, 남은시간 {gap_remaining}초){notice_str}", flush=True)
                else:
                    res = results[0] if results else None
                    power_w = res["power"] if res else 0.0
                    devs = res["devices"] if res else []
                    dev_str = ", ".join(devs) if devs else "대기전력(기저부하)"
                    print(f"[{now_iso}] (T+{cycle:03d}s) 소비전력: {power_w:6.1f} W | 상태: {status_tag:<22} | 가전: {dev_str}{notice_str}", flush=True)
            elif not args.quiet:
                active_info = [f"{r['house']}:{','.join(r['devices'])}" for r in results if r["devices"]]
                active_summary = f" [가전 ON: {'; '.join(active_info)}]" if active_info else ""
                print(f"[{now_iso}] (사이클 {cycle:04d}) {len(houses)}개 가구 발행 완료{active_summary}", flush=True)
            else:
                now = time.time()
                if now - last_summary_time >= 5.0 or (target_count > 0 and cycle >= target_count):
                    elapsed = max(0.001, now - last_summary_time)
                    batch_sent = total_sent - last_summary_sent
                    tps = batch_sent / elapsed
                    print(f"[{now_iso}] 진행 중: 사이클 {cycle}, 누적 {total_sent}건 (처리량: {tps:.1f} msg/s)", flush=True)
                    last_summary_time = now
                    last_summary_sent = total_sent

            # 목표 횟수 도달 시 종료
            if target_count > 0 and cycle >= target_count:
                break

            # 주기 보정 대기 (실제 전송 소요시간 차감)
            elapsed_in_cycle = time.time() - cycle_start
            sleep_time = max(0.0, interval - elapsed_in_cycle)
            await asyncio.sleep(sleep_time)

    total_elapsed = max(0.001, time.time() - start_time)
    avg_tps = total_sent / total_elapsed
    print(f"\n[완료] 총 {cycle}개 사이클, {total_sent}건 메시지 전송 완료 (평균 처리량: {avg_tps:.1f} msg/s)", flush=True)


def main():
    args = parse_args()
    try:
        asyncio.run(run_simulator(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n시뮬레이터를 정지합니다.", flush=True)
    except aiomqtt.MqttError as error:
        print(f"\nMQTT 연결 오류 발생: {error}", flush=True)
    except Exception as error:
        print(f"\n오류 발생: {error}", flush=True)


if __name__ == "__main__":
    main()
