"""
NILM 스마트홈 전력 시뮬레이터 관리자 모듈

SimulatorManager 클래스를 통해 시뮬레이터 백그라운드 워커의 라이프사이클(start/stop/restart),
동시성 제어(3단계 락: lifecycle_lock, simulation_lock, lock), MQTT 실시간 발행 루프,
SSE 구독자 관리 및 브로드캐스트를 전담합니다.
"""

import asyncio
import copy
from datetime import datetime, timezone, timedelta
import os
import queue
import sys
import threading
import time

# Windows SelectorLoop 호환성 설정
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 상위 디렉터리(infrastructure/mqtt/simulator) import 경로 등록
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

import aiomqtt
import scenarios
import simulator

try:
    from .config import validate_interval, DEFAULT_INTERVAL
except ImportError:
    from server.config import validate_interval, DEFAULT_INTERVAL


class ModeConflictError(Exception):
    """시뮬레이터가 실행 중이 아니거나 manual 모드가 아닐 때 발생하는 예외"""
    pass


class SimulatorManager:
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        tls_enabled: bool | None = None,
        ca_file: str | None = None,
    ):
        self.lock = threading.RLock()
        # HTTP 요청 스레드에서 동시에 들어오는 start/stop/set_device를 직렬화한다.
        self.lifecycle_lock = threading.Lock()
        # 시뮬레이션 상태 변경(가전 ON/OFF, 초기화) 및 물리 계측을 보호하는 락
        self.simulation_lock = threading.Lock()
        # 개별 1초 틱(메트릭 계산, MQTT 발행, SSE 브로드캐스트)의 원자성을 보장하는 락
        self.tick_lock = threading.Lock()
        self.is_running = False
        self.is_paused = False
        self.interval = DEFAULT_INTERVAL
        self.current_mode = "idle"  # 단일 가구는 해당 시나리오, 다중 가구는 "multi", 정지 시 "idle"
        self.simulation_date = None
        self.resolved_start_time = None
        self.seed = None  # 현재 실행의 난수 seed (같은 seed로 재시작하면 계측값이 재현됨)
        # 실행별로 별도 Event를 생성해 종료된 워커가 다시 살아나는 것을 막는다.
        self.stop_event = None
        self.worker_thread = None
        self.subscribers = []  # SSE 큐 목록
        self.last_metrics = None  # 가장 최근 브로드캐스트된 단일 이벤트 (하위 호환)
        self.last_metrics_by_house = {}  # 가구별 최신 이벤트의 authoritative 상태
        self.active_households = {}  # {house: {"scenario": ..., "cycle_count": ..., "status": ...}}
        self.global_cycle_count = 0
        self.cycle_count = 0  # global_cycle_count와 동일 (하위 호환)
        self._scenarios = {}  # {house: ScenarioInstance}
        self._appliance_states = {}  # {house: appliance_dict}

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.tls_enabled = tls_enabled
        self.ca_file = ca_file

    def get_connection_config(self) -> dict:
        """현재 설정된 인자 및 환경변수를 기반으로 연결 파라미터를 결정합니다."""
        from engine.tls import resolve_mqtt_config
        return resolve_mqtt_config(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            tls_enabled=self.tls_enabled,
            ca_file=self.ca_file,
        )

    def add_subscriber(self, q: queue.Queue):
        with self.lock:
            self.subscribers.append(q)

    def remove_subscriber(self, q: queue.Queue):
        with self.lock:
            if q in self.subscribers:
                self.subscribers.remove(q)

    def broadcast(self, data: dict):
        with self.lock:
            self.last_metrics = data
            for q in list(self.subscribers):
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass

    def broadcast_external(self, data: dict):
        """E2E 등 외부 세션의 화면 전송 전용 브로드캐스트.
        기존과 동일한 락 안에서 구독자 큐에만 데이터를 넣고,
        last_metrics 및 last_metrics_by_house는 절대 변경하지 않습니다.
        """
        with self.lock:
            for q in list(self.subscribers):
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass

    def start(
        self,
        scenario: str = "peak",
        house: str = "H001",
        simulation_date: str | None = None,
        *,
        households: list[dict] | None = None,
        interval: float | None = None,
        fault_duration_sec: int | None = None,
        seed: int | None = None,
        start_time: str | None = None,
    ) -> dict:
        """시뮬레이션을 시작합니다. 단일 가구 위치 인자 및 다중 가구 households keyword-only를 모두 지원합니다."""
        with self.lifecycle_lock:
            # 1. 단일 가구 하위 호환 및 households 목록 정규화
            if households is None:
                normalized_households = [{"house": house, "scenario": scenario}]
                is_multi = False
            else:
                normalized_households = sorted(households, key=lambda x: x["house"])
                is_multi = (len(normalized_households) > 1)

            # 1-A. 시연용 단일 가구 시나리오의 대상 검증
            for h_item in normalized_households:
                if h_item.get("scenario") == "normal_routine" and h_item.get("house") != "H001":
                    raise ValueError("normal_routine 시나리오는 H001 가구에서만 실행할 수 있습니다.")
                if h_item.get("scenario") == "prolonged_use" and h_item.get("house") != "H001":
                    raise ValueError("prolonged_use 시나리오는 H001 가구에서만 실행할 수 있습니다.")
                if h_item.get("scenario") == "routine_missed_demo" and h_item.get("house") != "H001":
                    raise ValueError("routine_missed_demo 시나리오는 H001 가구에서만 실행할 수 있습니다.")
            if is_multi and any(h["scenario"] == "routine_missed_demo" for h in normalized_households):
                raise ValueError("routine_missed_demo 시나리오는 단일 가구에서만 실행할 수 있습니다.")

            # 2. TLS 설정 사전 동기 검증 (CA 누락/미존재/권한 오류 시 런타임 start 호출에서 즉각 예외 발생)
            from engine.tls import get_mqtt_tls_context
            cfg = self.get_connection_config()
            tls_context = get_mqtt_tls_context(tls_enabled=cfg["tls_enabled"], ca_file=cfg["ca_file"])

            # 3. 공통 가상 시작 시각 결정
            has_normal = any(h["scenario"] == "normal_routine" for h in normalized_households)
            has_missed = any(h["scenario"] in ("routine_missed", "routine_missed_demo") for h in normalized_households)
            if has_normal and has_missed:
                raise ValueError("normal_routine과 routine_missed는 동일한 다중 실행에서 함께 사용할 수 없습니다.")
            has_prolonged = any(h["scenario"] == "prolonged_use" for h in normalized_households)
            if has_prolonged and (has_normal or has_missed):
                raise ValueError("prolonged_use는 normal_routine·routine_missed와 동일한 다중 실행에서 함께 사용할 수 없습니다.")

            if not is_multi and normalized_households[0]["scenario"] == "routine_missed":
                base_dt = scenarios.resolve_simulation_start_time(
                    "routine_missed",
                    simulation_date=simulation_date,
                    routine_default_time="08:10:01"
                )
            elif not is_multi and normalized_households[0]["scenario"] == "routine_missed_demo":
                # 날짜 미지정 시 첫 계측 tick의 현재 시각을 사용한다.
                # 날짜를 명시한 API 호출은 그 날짜의 현재 KST 시각을 따른다.
                base_dt = scenarios.resolve_simulation_start_time(
                    "manual", simulation_date=simulation_date
                )
            elif not is_multi and normalized_households[0]["scenario"] == "normal_routine":
                base_dt = scenarios.resolve_simulation_start_time(
                    "normal_routine",
                    simulation_date=simulation_date,
                    routine_default_time=scenarios.NormalRoutineScenario.DEFAULT_START_TIME
                )
            elif not is_multi and normalized_households[0]["scenario"] == "prolonged_use":
                base_dt = scenarios.resolve_simulation_start_time(
                    "prolonged_use",
                    simulation_date=simulation_date,
                )
            else:
                base_dt = scenarios.resolve_multi_simulation_start_time(
                    normalized_households,
                    simulation_date=simulation_date,
                    routine_default_time="08:10:01"
                )

            # 3-0. 시작 시각 고정: 지정 시 선택 날짜(없으면 오늘) + start_time(KST)으로 base_dt를 덮어쓴다.
            #      루틴 시나리오는 타임라인이 시작 시각에 묶여 있으므로 허용하지 않는다.
            if start_time is not None:
                if has_normal or has_missed or has_prolonged:
                    raise ValueError("normal_routine/routine_missed/prolonged_use 시나리오는 시작 시각이 고정되어 있어 start_time을 지정할 수 없습니다.")
                base_dt = scenarios.resolve_fixed_start_time(start_time, simulation_date=simulation_date)

            resolved_start_iso = scenarios.format_iso_utc(base_dt) if base_dt else None

            # 3-A. interval 사전 검증: 기존 워커를 중지하기 전에 검증하여 잘못된 interval 시 기존 워커를 보호한다.
            if interval is None:
                effective_interval = DEFAULT_INTERVAL
            else:
                effective_interval = validate_interval(interval)

            # 3-B. fault_duration_sec 사전 검증: 기존 워커를 중지하기 전에 검증하여 잘못된 인자 시 기존 워커를 보호한다.
            has_sensor_fault = any(h["scenario"] == "sensor_fault" for h in normalized_households)
            if fault_duration_sec is not None:
                if type(fault_duration_sec) is not int or isinstance(fault_duration_sec, bool):
                    raise ValueError("fault_duration_sec는 1~3600 사이의 정수여야 합니다.")
                if not (1 <= fault_duration_sec <= 3600):
                    raise ValueError("fault_duration_sec는 1~3600 범위의 정수여야 합니다.")
                if not has_sensor_fault:
                    raise ValueError("sensor_fault 시나리오가 포함되지 않은 요청에는 fault_duration_sec를 지정할 수 없습니다.")
                effective_fault_duration = fault_duration_sec
            else:
                effective_fault_duration = scenarios.SensorFaultScenario.DEFAULT_FAULT_DURATION_SEC if has_sensor_fault else None

            # 3-C. seed 사전 검증 및 생성: 기존 워커를 중지하기 전에 검증한다.
            effective_seed = simulator.resolve_seed(seed)

            # 각 가구 설정에 fault_duration_sec 바인딩
            for h in normalized_households:
                if h["scenario"] == "sensor_fault":
                    h["fault_duration_sec"] = effective_fault_duration

            if not self._stop_and_join():
                raise RuntimeError("기존 시뮬레이터 워커가 종료되지 않았습니다.")

            # 4. 이전 워커가 완전히 종료된 후, simulation_lock 안에서 대상 가구들의 상태 머신 초기화
            target_houses = [h["house"] for h in normalized_households]
            with self.simulation_lock:
                simulator.init_simulation_states(target_houses, seed=effective_seed)

            stop_event = threading.Event()
            worker_thread = threading.Thread(
                target=self._run_async_worker,
                args=(normalized_households, stop_event, cfg, tls_context, simulation_date, base_dt),
                daemon=True
            )

            current_mode = "multi" if is_multi else normalized_households[0]["scenario"]

            with self.lock:
                self.stop_event = stop_event
                self.worker_thread = worker_thread
                self.is_running = True
                self.is_paused = False
                self.interval = effective_interval
                self.current_mode = current_mode
                self.house = normalized_households[0]["house"]
                self.global_cycle_count = 0
                self.cycle_count = 0
                self.simulation_date = simulation_date
                self.resolved_start_time = resolved_start_iso
                self.seed = effective_seed
                self.active_households = {
                    h["house"]: {
                        "scenario": h["scenario"],
                        "cycle_count": 0,
                        "status": "running",
                        "fault_duration_sec": h.get("fault_duration_sec"),
                    }
                    for h in normalized_households
                }
                self.last_metrics_by_house = {}
                self.last_metrics = None

            worker_thread.start()
            resp = {
                "status": "started",
                "scenario": current_mode,
                "house": normalized_households[0]["house"],
                "households": normalized_households,
                "simulation_date": simulation_date,
                "resolved_start_time": resolved_start_iso,
                "interval": effective_interval,
                "speed": round(1.0 / effective_interval, 2),
                "seed": effective_seed,
            }
            if has_sensor_fault:
                resp["fault_duration_sec"] = effective_fault_duration
            return resp

    def set_device(self, house: str, device: str, enabled: bool) -> dict:
        """가전 상태를 수동 변경한다. lifecycle_lock 및 simulation_lock 안에서 원자적으로 검증 및 상태 변경."""
        with self.lifecycle_lock:
            with self.simulation_lock:
                with self.lock:
                    if not self.is_running:
                        raise ModeConflictError("시뮬레이터가 정지 상태(idle)입니다. 먼저 시뮬레이션을 시작하세요.")
                    if house not in self.active_households:
                        raise ModeConflictError(f"가구 '{house}'는 현재 실행 중인 가구 목록에 포함되어 있지 않습니다.")
                    h_info = self.active_households[house]
                    if h_info["status"] != "running":
                        raise ModeConflictError(f"가구 '{house}'는 현재 실행 중이 아닙니다 (현재 상태: {h_info['status']}).")
                    if h_info["scenario"] != "manual":
                        raise ModeConflictError(f"가전 수동 제어는 'manual' 시나리오 가구에서만 가능합니다. (가구 '{house}' 시나리오: {h_info['scenario']})")

                return simulator.set_manual_device_state(house, device, enabled)

    def pause(self) -> dict:
        """시뮬레이터를 일시정지합니다. lifecycle_lock 및 self.lock으로 보호됩니다."""
        with self.lifecycle_lock:
            with self.lock:
                if not self.is_running:
                    raise ModeConflictError("시뮬레이터가 실행 중이 아닙니다.")
                if self.is_paused:
                    return {"status": "paused"}
                self.is_paused = True

            # 진행 중인 틱(MQTT/SSE 발행)이 있다면 완전히 완료될 때까지 대기
            with self.tick_lock:
                pass

            return {"status": "paused"}

    def resume(self) -> dict:
        """일시정지된 시뮬레이터를 재개합니다. lifecycle_lock 및 self.lock으로 보호됩니다."""
        with self.lifecycle_lock:
            with self.lock:
                if not self.is_running:
                    raise ModeConflictError("시뮬레이터가 실행 중이 아닙니다.")
                if not self.is_paused:
                    return {"status": "resumed"}
                self.is_paused = False
            return {"status": "resumed"}

    def set_interval(self, interval: float) -> dict:
        """실행 중인 시뮬레이션의 발행 주기(배속)를 동적으로 변경합니다."""
        valid_interval = validate_interval(interval)
        with self.lock:
            if not self.is_running:
                raise ModeConflictError("시뮬레이터가 실행 중이 아닙니다.")
            self.interval = valid_interval
            speed = round(1.0 / valid_interval, 2)
            return {
                "status": "speed_updated",
                "interval": valid_interval,
                "speed": speed,
            }

    def reset(self) -> dict:
        """시뮬레이터를 정지하고 모든 가구 상태, 메트릭, 날짜를 초기화합니다."""
        with self.lifecycle_lock:
            if not self._stop_and_join():
                raise RuntimeError("시뮬레이터 워커 종료에 실패하여 리셋할 수 없습니다.")
            with self.lock:
                self.interval = DEFAULT_INTERVAL
                self.active_households = {}
                self.last_metrics_by_house = {}
                self.last_metrics = None
                self.simulation_date = None
                self.resolved_start_time = None
                self.seed = None
                self.global_cycle_count = 0
                self.cycle_count = 0
                self.is_paused = False
            return {"status": "reset"}

    def get_status(self) -> dict:
        with self.tick_lock:
            with self.lock:
                running_exists = any(
                    h.get("status") == "running"
                    for h in self.active_households.values()
                ) if self.active_households else self.is_running
                return {
                    "is_running": running_exists,
                    "is_paused": self.is_paused,
                    "interval": self.interval,
                    "speed": round(1.0 / self.interval, 2),
                    "current_mode": self.current_mode,
                    "scenario": self.current_mode,
                    "cycle_count": self.cycle_count,
                    "global_cycle_count": self.global_cycle_count,
                    "house": getattr(self, "house", "H001"),
                    "active_households": copy.deepcopy(self.active_households),
                    "last_metrics_by_house": copy.deepcopy(self.last_metrics_by_house),
                    "last_metrics": copy.deepcopy(self.last_metrics) if self.last_metrics else None,
                    "simulation_date": self.simulation_date,
                    "resolved_start_time": self.resolved_start_time,
                    "seed": self.seed,
                }

    def stop(self):
        with self.lifecycle_lock:
            return self._stop_and_join()

    def _stop_and_join(self) -> bool:
        """현재 워커에 종료를 요청하고 완전히 종료될 때까지 기다린다."""
        with self.lock:
            worker_thread = self.worker_thread
            stop_event = self.stop_event
            self.is_paused = False

            if stop_event is not None:
                stop_event.set()

        # worker의 finally도 self.lock을 사용하므로 lock 밖에서 기다린다.
        if (
            worker_thread is not None
            and worker_thread.is_alive()
            and worker_thread is not threading.current_thread()
        ):
            worker_thread.join(timeout=10.0)

        if worker_thread is not None and worker_thread.is_alive():
            return False

        with self.lock:
            # 자연 종료 직후 다른 상태가 등록된 경우 이를 덮어쓰지 않는다.
            if self.worker_thread is worker_thread:
                self.worker_thread = None
                self.stop_event = None
                self.is_running = False
                self.is_paused = False
                self.current_mode = "idle"
                # 명시적 stop 시 running이던 가구만 stopped로 변경하고, completed 가구는 상태 보존
                for h_info in self.active_households.values():
                    if h_info["status"] == "running":
                        h_info["status"] = "stopped"

        return True

    def _run_async_worker(
        self,
        households: list[dict],
        stop_event: threading.Event,
        cfg: dict,
        tls_context,
        simulation_date: str | None = None,
        base_dt: datetime | None = None
    ):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                self._worker_loop(households, stop_event, cfg, tls_context, simulation_date, base_dt)
            )
        except Exception as err:
            print(f"[WebSimulator] 워커 오류: {err}", flush=True)
        finally:
            loop.close()
            with self.lock:
                # 과거 워커가 새 워커의 실행 상태를 덮어쓰지 않도록 보호한다.
                if self.worker_thread is threading.current_thread():
                    self.worker_thread = None
                    self.stop_event = None
                    self.is_running = False
                    self.is_paused = False
                    self.current_mode = "idle"
                    # 자연 완주 시 running이던 가구만 stopped로 마킹하고 completed 상태 및 메트릭 보존
                    for h_info in self.active_households.values():
                        if h_info["status"] == "running":
                            h_info["status"] = "stopped"

    async def _worker_loop(
        self,
        target_or_scenario,
        house_or_stop_event,
        stop_event_or_cfg,
        cfg_or_tls_context,
        tls_or_sim_date=None,
        base_dt_or_none=None,
        *extra_args,
        **extra_kwargs
    ):
        if isinstance(target_or_scenario, str):
            # 레거시 단일 가구 호출 호환: _worker_loop(scenario, house, stop_event, cfg, tls_context)
            legacy_scenario = target_or_scenario
            legacy_house = house_or_stop_event
            stop_event = stop_event_or_cfg
            cfg = cfg_or_tls_context
            tls_context = tls_or_sim_date
            simulation_date = base_dt_or_none
            base_dt = extra_args[0] if len(extra_args) > 0 else extra_kwargs.get("base_dt")
            households = [{"house": legacy_house, "scenario": legacy_scenario}]
            with self.lock:
                self.house = legacy_house
                self.current_mode = legacy_scenario
                if legacy_house not in self.active_households:
                    self.active_households[legacy_house] = {
                        "scenario": legacy_scenario,
                        "status": "running",
                        "cycle_count": 0,
                    }
            with self.simulation_lock:
                simulator.init_simulation_states([legacy_house])
        else:
            # 정규 다중 가구 호출: _worker_loop(households, stop_event, cfg, tls_context, simulation_date, base_dt)
            households = target_or_scenario
            stop_event = house_or_stop_event
            cfg = stop_event_or_cfg
            tls_context = cfg_or_tls_context
            simulation_date = tls_or_sim_date
            base_dt = base_dt_or_none

        host = cfg["host"]
        port = cfg["port"]
        user = cfg["username"]
        password = cfg["password"]
        tls_enabled = cfg["tls_enabled"]
        ca_file = cfg["ca_file"]

        tls_desc = f" (TLS ON | CA: {ca_file})" if tls_enabled else " (평문)"
        print(f"[WebSimulator] MQTT 브로커({host}:{port}{tls_desc}) 연결 중...", flush=True)

        async with aiomqtt.Client(
            hostname=host,
            port=port,
            username=user,
            password=password,
            tls_context=tls_context,
            keepalive=60,
            timeout=5
        ) as client:
            client.pending_calls_threshold = max(len(households) * 4, 200)
            houses_desc = ", ".join(f"{h['house']}({h['scenario']})" for h in households)
            print(f"[WebSimulator] MQTT 연결 성공 실시간 발행 시작 [{houses_desc}]", flush=True)

            cycle = 0
            internal_base_dt = base_dt
            while not stop_event.is_set():
                # 0. 일시정지 상태 확인 (Manager 락으로 보호)
                with self.lock:
                    if self.is_paused:
                        is_paused_now = True
                    else:
                        is_paused_now = False

                if is_paused_now:
                    await asyncio.sleep(0.05)
                    continue

                cycle_started = time.monotonic()

                # tick_lock으로 전체 틱(메트릭 계산, MQTT 발행, SSE 브로드캐스트)을 보호하여
                # pause() 및 get_status() 호출 시 현재 틱이 완전히 끝난 후 일관된 상태가 조회되도록 보장한다.
                with self.tick_lock:
                    # 1. 실행 중(running)인 가구 확인 및 다음 cycle 번호 산정
                    with self.lock:
                        if self.is_paused:
                            continue
                        running_items = [
                            h for h in households
                            if self.active_households.get(h["house"], {}).get("status") == "running"
                        ]
                        if not running_items:
                            print(f"[WebSimulator] 모든 가구 시뮬레이션 완주, 워커를 자동 정지합니다.", flush=True)
                            break
                        house_next_cycles = {
                            h["house"]: self.active_households[h["house"]]["cycle_count"] + 1
                            for h in running_items
                        }

                    cycle += 1

                    # 2. 첫 계측 tick을 실제로 생성하는 시점에 internal_base_dt를 1회만 확정
                    #    (연결 지연이나 첫 tick 이전의 pause가 가상 시각에 반영되지 않음)
                    if internal_base_dt is None:
                        internal_base_dt = datetime.now(timezone.utc)

                    sim_dt = internal_base_dt + timedelta(seconds=cycle - 1)
                    now_iso = scenarios.format_iso_utc(sim_dt)
                    sim_kst = sim_dt.astimezone(scenarios.KST)
                    sim_time_kst = sim_kst.strftime("%H:%M:%S")
                    sim_date_kst = sim_kst.strftime("%Y-%m-%d")
                    sim_datetime_kst = sim_kst.strftime("%Y-%m-%d %H:%M:%S KST")

                    # 3. simulation_lock 하에서 가구별 이벤트 주입, 물리 계측, 디바이스 스냅샷 생성
                    #    (이 단계에서는 active_households를 갱신하지 않고 임시 계산만 수행)
                    tick_calc_results = []
                    with self.simulation_lock:
                        for item in running_items:
                            house = item["house"]
                            scenario = item["scenario"]
                            h_cycle = house_next_cycles[house]
                            allow_random = (scenario == "random")

                            event_desc = None
                            if scenario == "peak":
                                event_desc = simulator.inject_peak_scenario_event(h_cycle, house)
                            elif scenario == "routine_missed":
                                if h_cycle == 1:
                                    event_desc = "08:10 아침 루틴 검증 시작 (전자레인지 미가동 / 대기전력 유지)"
                                elif h_cycle == 100:
                                    event_desc = "대기전력 지속 중 (누적 100초)"
                                elif h_cycle == 200:
                                    event_desc = "대기전력 지속 중 (누적 200초)"
                                elif h_cycle == scenarios.RoutineMissedScenario.BUFFER_WINDOW_SIZE:
                                    event_desc = "299개 분석 입력 데이터 충족 — AI 이상 감지 판정 대기"
                                elif h_cycle == 300:
                                    event_desc = "H001 루틴 누락 전력 패턴 발행 완료 (300초 데이터 전송 완료)"
                            elif scenario == "routine_missed_demo":
                                if h_cycle == 1:
                                    event_desc = "평소 사용 시간대 비교 시작 (전자레인지 미가동 / 대기전력 유지)"
                                elif h_cycle == 92:
                                    event_desc = "평소 사용 마감 시각 경과 (전자레인지 미사용)"
                                elif h_cycle == scenarios.RoutineMissedDemoScenario.TOTAL_CYCLES:
                                    event_desc = "H001 루틴 누락 비교 전력 패턴 발행 완료"
                            elif scenario == "normal_routine":
                                event_desc = simulator.inject_normal_routine_scenario_event(h_cycle, house)
                                if not event_desc:
                                    if h_cycle == 1:
                                        event_desc = "08:04:58 아침 정상 루틴 시뮬레이션 시작 (대기전력 유지)"
                                    elif h_cycle == scenarios.NormalRoutineScenario.TOTAL_CYCLES:
                                        event_desc = "H001 정상 일상 전력 패턴 발행 완료"
                            elif scenario == "prolonged_use":
                                event_desc = simulator.inject_prolonged_use_scenario_event(h_cycle, house)
                                if not event_desc and h_cycle == 1:
                                    event_desc = "11:55 점심 사용시간 초과 시나리오 시작 (대기전력 유지)"
                            elif scenario == "sensor_fault":
                                fault_dur = item.get("fault_duration_sec") or scenarios.SensorFaultScenario.DEFAULT_FAULT_DURATION_SEC
                                tl = scenarios.SensorFaultScenario.get_timeline(fault_dur)
                                if h_cycle == tl.fault_start:
                                    event_desc = f"센서 고장 시작 — 전력 계측 MQTT 메시지 발행 중단 ({tl.duration_sec}초간 결측)"
                                elif h_cycle == tl.recovery_start:
                                    event_desc = "센서 복구 — 전력 계측 데이터 정상 발행 재개"
                                elif h_cycle >= tl.total_cycles:
                                    event_desc = f"시나리오 완료 — 센서 고장 및 복구 시연 완료 (총 {tl.total_cycles}초)"

                            fault_dur = item.get("fault_duration_sec") or scenarios.SensorFaultScenario.DEFAULT_FAULT_DURATION_SEC
                            is_fault = (scenario == "sensor_fault" and scenarios.SensorFaultScenario.is_fault_cycle(h_cycle, duration_sec=fault_dur))
                            metrics = simulator.calculate_main_panel_metrics(house, allow_random=allow_random)

                            devices_snapshot = {}
                            house_devices = simulator.device_states.get(house, {})
                            for dev_name in simulator.DEVICE_PROFILES:
                                dev_st = house_devices.get(dev_name, {})
                                st_state = dev_st.get("state", "OFF")
                                devices_snapshot[dev_name] = {
                                    "state": st_state,
                                    "enabled": (st_state != "OFF"),
                                    "manualHold": bool(dev_st.get("manual_hold", False))
                                }

                            tick_calc_results.append({
                                "house": house,
                                "scenario": scenario,
                                "h_cycle": h_cycle,
                                "is_fault": is_fault,
                                "fault_dur": fault_dur if scenario == "sensor_fault" else None,
                                "event_desc": event_desc,
                                "metrics": metrics,
                                "devices_snapshot": devices_snapshot
                            })

                    # 4. simulation_lock 해제 후 가구별 MQTT 동시 발행 (네트워크 I/O 중 lock 미보유)
                    #    sensor_fault 고장 가구는 MQTT 발행을 건너뜀 (0건 발행)
                    mqtt_pub_tasks = [
                        simulator.publish_house_power(
                            client=client,
                            house=res["house"],
                            now_iso=now_iso,
                            qos=1,
                            allow_random=False,
                            metrics=res["metrics"]
                        )
                        for res in tick_calc_results
                        if not res.get("is_fault", False)
                    ]
                    if mqtt_pub_tasks:
                        await asyncio.gather(*mqtt_pub_tasks)

                    # 5. 한 tick의 모든 공개 상태(cycle, status, last_metrics)를 단일 락 안에서 원자적으로 일괄 커밋(batch commit)
                    broadcast_items = []
                    with self.lock:
                        self.global_cycle_count = cycle
                        self.cycle_count = cycle

                        for res in tick_calc_results:
                            house = res["house"]
                            scenario = res["scenario"]
                            h_cycle = res["h_cycle"]
                            is_fault = res.get("is_fault", False)
                            metrics = res["metrics"]
                            event_desc = res["event_desc"]
                            devices_snapshot = res["devices_snapshot"]

                            # 완료 여부 판정
                            is_completed = False
                            if scenario == "peak" and h_cycle >= 60:
                                is_completed = True
                            elif scenario == "routine_missed" and h_cycle >= 300:
                                is_completed = True
                            elif scenario == "routine_missed_demo" and h_cycle >= scenarios.RoutineMissedDemoScenario.TOTAL_CYCLES:
                                is_completed = True
                            elif scenario == "normal_routine" and h_cycle >= scenarios.NormalRoutineScenario.TOTAL_CYCLES:
                                is_completed = True
                            elif scenario == "prolonged_use" and h_cycle >= scenarios.ProlongedUseScenario.TOTAL_CYCLES:
                                is_completed = True
                            elif scenario == "sensor_fault":
                                fault_dur = res.get("fault_dur") or self.active_households[house].get("fault_duration_sec") or scenarios.SensorFaultScenario.DEFAULT_FAULT_DURATION_SEC
                                if h_cycle >= scenarios.get_scenario_target_cycles("sensor_fault", fault_dur):
                                    is_completed = True

                            self.active_households[house]["cycle_count"] = h_cycle
                            if is_completed:
                                self.active_households[house]["status"] = "completed"
                            h_status = self.active_households[house]["status"]

                            # SSE 데이터 생성: 고장 구간에는 전력/전압/전류 필드를 null로 설정하고 위조하지 않음
                            fault_dur = (res.get("fault_dur") or self.active_households[house].get("fault_duration_sec") or scenarios.SensorFaultScenario.DEFAULT_FAULT_DURATION_SEC) if scenario == "sensor_fault" else None
                            if scenario == "sensor_fault":
                                _, gap_elapsed, gap_remaining = scenarios.SensorFaultScenario.get_gap_metrics(h_cycle, duration_sec=fault_dur)
                            else:
                                gap_elapsed = None
                                gap_remaining = None

                            if is_fault:
                                broadcast_data = {
                                    "sec": h_cycle,
                                    "cycle_count": h_cycle,
                                    "house": house,
                                    "scenario": scenario,
                                    "mode": scenario,
                                    "status": h_status,
                                    "now_iso": now_iso,
                                    "simTimeKst": sim_time_kst,
                                    "simDateKst": sim_date_kst,
                                    "simDateTimeKst": sim_datetime_kst,
                                    "measurementAvailable": False,
                                    "sensorFault": True,
                                    "faultDurationSec": fault_dur,
                                    "gapElapsedSec": gap_elapsed,
                                    "gapRemainingSec": gap_remaining,
                                    "totalP": None,
                                    "totalQ": None,
                                    "apparentS": None,
                                    "pf": None,
                                    "voltage": None,
                                    "currentA": None,
                                    "activeNames": [],
                                    "eventNoticeText": event_desc,
                                    "devices": devices_snapshot
                                }
                            else:
                                broadcast_data = {
                                    "sec": h_cycle,
                                    "cycle_count": h_cycle,
                                    "house": house,
                                    "scenario": scenario,
                                    "mode": scenario,
                                    "status": h_status,
                                    "now_iso": now_iso,
                                    "simTimeKst": sim_time_kst,
                                    "simDateKst": sim_date_kst,
                                    "simDateTimeKst": sim_datetime_kst,
                                    "measurementAvailable": True,
                                    "sensorFault": False,
                                    "faultDurationSec": fault_dur,
                                    "gapElapsedSec": gap_elapsed,
                                    "gapRemainingSec": gap_remaining,
                                    "totalP": metrics["active_power"],
                                    "totalQ": metrics["reactive_power"],
                                    "apparentS": metrics["apparent_power"],
                                    "pf": metrics["power_factor"],
                                    "voltage": metrics["voltage"],
                                    "currentA": metrics["current"],
                                    "activeNames": metrics["active_devices"],
                                    "eventNoticeText": event_desc,
                                    "devices": devices_snapshot
                                }

                            self.last_metrics_by_house[house] = broadcast_data
                            self.last_metrics = broadcast_data
                            broadcast_items.append((broadcast_data, res, h_status))

                    # 6. SSE 브로드캐스트 및 터미널 로그 출력
                    for broadcast_data, res, h_status in broadcast_items:
                        self.broadcast(broadcast_data)

                        scenario = res["scenario"]
                        h_cycle = res["h_cycle"]
                        is_fault = res.get("is_fault", False)
                        metrics = res["metrics"]
                        event_desc = res["event_desc"]
                        house = res["house"]

                        # 터미널 로그 출력: sensor_fault 고장 구간에는 내부 계산값을 출력하지 않고 [센서 고장 / 측정 없음]으로 표시
                        if scenario == "peak":
                            status_tag = "대기"
                            if metrics["active_power"] >= 3000.0:
                                status_tag = "피크 경보 (3,000W+)"
                            elif metrics["active_power"] >= 1000.0:
                                status_tag = "가전 가동 중"
                            notice_str = f" <== [{event_desc}]" if event_desc else ""
                            print(f"[WebSimulator] (T+{h_cycle:02d}s | {sim_datetime_kst}) {house} 전력: {metrics['active_power']:7.1f} W | {status_tag} [{h_status}]{notice_str}", flush=True)
                        elif scenario in ("routine_missed", "routine_missed_demo"):
                            status_tag, notice = scenarios.RoutineMissedScenario.get_cycle_status(h_cycle)
                            notice_str = f" <== [{event_desc}]" if event_desc else notice
                            print(f"[WebSimulator] (T+{h_cycle:03d}s | {sim_datetime_kst}) {house} 전력: {metrics['active_power']:5.1f} W | {status_tag} [{h_status}]{notice_str}", flush=True)
                        elif scenario == "normal_routine":
                            status_tag = "전자레인지 가동 중" if "전자레인지" in metrics["active_devices"] else "정상 대기"
                            notice_str = f" <== [{event_desc}]" if event_desc else ""
                            print(f"[WebSimulator] (T+{h_cycle:03d}s | {sim_datetime_kst}) {house} 전력: {metrics['active_power']:7.1f} W | {status_tag} [{h_status}]{notice_str}", flush=True)
                        elif scenario == "prolonged_use":
                            status_tag = "전자레인지 가동 중" if res["devices_snapshot"].get("microwave", {}).get("enabled") else "대기"
                            notice_str = f" <== [{event_desc}]" if event_desc else ""
                            print(f"[WebSimulator] (T+{h_cycle:03d}s | {sim_datetime_kst}) {house} 전력: {metrics['active_power']:7.1f} W | {status_tag} [{h_status}]{notice_str}", flush=True)
                        elif scenario == "sensor_fault":
                            notice_str = f" <== [{event_desc}]" if event_desc else ""
                            fault_dur = res.get("fault_dur") or self.active_households[house].get("fault_duration_sec") or scenarios.SensorFaultScenario.DEFAULT_FAULT_DURATION_SEC
                            tl = scenarios.SensorFaultScenario.get_timeline(fault_dur)
                            _, gap_elapsed, gap_remaining = scenarios.SensorFaultScenario.get_gap_metrics(h_cycle, duration_sec=fault_dur)
                            if is_fault:
                                print(f"[WebSimulator] (T+{h_cycle:03d}s | {sim_datetime_kst}) {house} [센서 고장 / 측정 없음] MQTT 미발행 | 센서 고장 ({gap_elapsed}/{tl.duration_sec}초, 남은시간 {gap_remaining}초) [{h_status}]{notice_str}", flush=True)
                            else:
                                recov_tag = "센서 복구 (정상 계측)" if h_cycle >= tl.recovery_start else "정상 대기 계측"
                                print(f"[WebSimulator] (T+{h_cycle:03d}s | {sim_datetime_kst}) {house} 전력: {metrics['active_power']:7.1f} W | {recov_tag} [{h_status}]{notice_str}", flush=True)
                        elif scenario == "manual":
                            act_str = ", ".join(metrics["active_devices"]) if metrics["active_devices"] else "대기(가전 OFF)"
                            print(f"[WebSimulator] (T+{h_cycle:02d}s | {sim_datetime_kst}) {house} 전력: {metrics['active_power']:7.1f} W | 수동 [{act_str}] [{h_status}]", flush=True)
                        else:
                            status_tag = "가전 가동 중" if metrics["active_power"] >= 500.0 else "대기"
                            print(f"[WebSimulator] (T+{h_cycle:02d}s | {sim_datetime_kst}) {house} 전력: {metrics['active_power']:7.1f} W | {status_tag} [{h_status}]", flush=True)

                # 모든 가구가 완료되었는지 확인
                with self.lock:
                    still_running = any(h["status"] == "running" for h in self.active_households.values())
                    if not still_running:
                        print(f"[WebSimulator] 모든 가구 시뮬레이션 완주, 워커를 자동 정지합니다.", flush=True)
                        break

                await self._sleep_until_next_cycle(cycle_started, stop_event)

    async def _sleep_until_next_cycle(self, cycle_started: float, stop_event: threading.Event) -> None:
        """현재 interval을 동적으로 반영하여 다음 사이클 마감 시각까지 분할 대기한다."""
        while not stop_event.is_set():
            with self.lock:
                current_interval = self.interval
                paused = self.is_paused

            if paused:
                break

            remaining = cycle_started + current_interval - time.monotonic()
            if remaining <= 0:
                break

            await asyncio.sleep(min(0.05, remaining))
