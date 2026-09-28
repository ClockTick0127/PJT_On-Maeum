"""
NILM 전력 시뮬레이터 호환성 및 비네트워크 회귀 단위 테스트
"""

import copy
import json
import math
import os
import random
import sys
import unittest

# 상위 시뮬레이터 디렉터리 import 경로 등록
SIMULATOR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SIMULATOR_DIR not in sys.path:
    sys.path.insert(0, SIMULATOR_DIR)

import simulator
import engine
import engine.config
import engine.profiles
import engine.state
import engine.power_model
import engine.publisher


def serialize_val(val):
    if isinstance(val, (int, float, str, bool)) or val is None:
        return val
    if isinstance(val, dict):
        return {k: serialize_val(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [serialize_val(v) for v in val]
    return str(val)


class TestSimulatorCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 모듈 로드 시점의 기본 10가구 원본 상태 보존
        cls._initial_houses_house_states = copy.deepcopy(simulator.house_states)
        cls._initial_houses_device_states = copy.deepcopy(simulator.device_states)

    @classmethod
    def tearDownClass(cls):
        # 전체 테스트 완료 후 기본 10가구 원본 상태 복원 (객체 identity 유지)
        simulator.house_states.clear()
        simulator.house_states.update(cls._initial_houses_house_states)
        simulator.device_states.clear()
        simulator.device_states.update(cls._initial_houses_device_states)

    def setUp(self):
        # 테스트 격리를 위해 매 테스트 전 random state 및 상태 보존
        self._orig_random_state = random.getstate()
        self._orig_house_states = copy.deepcopy(simulator.house_states)
        self._orig_device_states = copy.deepcopy(simulator.device_states)

        # 각 테스트는 H001 격리 상태에서 시작
        simulator.init_simulation_states(["H001"])

    def tearDown(self):
        # dict 객체 자체를 재할당하지 않고 clear/update로 원본 내용 복원
        simulator.house_states.clear()
        simulator.house_states.update(self._orig_house_states)
        simulator.device_states.clear()
        simulator.device_states.update(self._orig_device_states)

        random.setstate(self._orig_random_state)

    def _assert_deep_equal(self, expected, actual, path=""):
        """중첩 dict, list, float(rel_tol=0.0, abs_tol=1e-6) 재귀적 엄격 동등성 검증"""
        if isinstance(expected, float) and isinstance(actual, (float, int)):
            self.assertTrue(
                math.isclose(expected, float(actual), rel_tol=0.0, abs_tol=1e-6),
                f"Float 불일치 ({path}): expected={expected}, actual={actual}"
            )
        elif isinstance(expected, (int, str, bool)) or expected is None:
            self.assertEqual(
                expected, actual,
                f"값 불일치 ({path}): expected={expected!r}, actual={actual!r}"
            )
        elif isinstance(expected, dict):
            self.assertIsInstance(actual, dict, f"타입 불일치 ({path}): expected dict, got {type(actual)}")
            expected_keys = set(expected.keys())
            actual_keys = set(actual.keys())
            self.assertEqual(
                expected_keys, actual_keys,
                f"딕셔너리 키 불일치 ({path}): 누락={expected_keys - actual_keys}, 초과={actual_keys - expected_keys}"
            )
            for k in expected:
                self._assert_deep_equal(expected[k], actual[k], path=f"{path}.{k}" if path else str(k))
        elif isinstance(expected, (list, tuple)):
            self.assertIsInstance(actual, (list, tuple), f"타입 불일치 ({path}): expected list/tuple, got {type(actual)}")
            self.assertEqual(
                len(expected), len(actual),
                f"리스트 길이 불일치 ({path}): expected {len(expected)}, got {len(actual)}"
            )
            for idx, (exp_item, act_item) in enumerate(zip(expected, actual)):
                self._assert_deep_equal(exp_item, act_item, path=f"{path}[{idx}]")
        else:
            self.assertEqual(expected, actual, f"기타 타입 불일치 ({path})")

    def test_01_public_symbols_existence(self):
        """기존 simulator 모듈의 필수 공개 심볼 존재 확인"""
        required_constants = [
            "DEFAULT_BROKER_HOST",
            "DEFAULT_BROKER_PORT",
            "DEFAULT_BROKER_USER",
            "DEFAULT_BROKER_PASS",
            "DEFAULT_HOUSES",
            "DEVICE_PROFILES",
        ]
        for name in required_constants:
            self.assertTrue(hasattr(simulator, name), f"Missing constant: {name}")

        required_states = ["house_states", "device_states"]
        for name in required_states:
            self.assertTrue(hasattr(simulator, name), f"Missing state: {name}")

        required_functions = [
            "init_simulation_states",
            "set_manual_device_state",
            "inject_peak_scenario_event",
            "update_house_environment",
            "update_and_generate_device_load",
            "calculate_main_panel_metrics",
            "publish_house_power",
            "parse_args",
            "run_simulator",
            "main",
        ]
        for name in required_functions:
            self.assertTrue(hasattr(simulator, name), f"Missing function: {name}")
            self.assertTrue(callable(getattr(simulator, name)), f"Not callable: {name}")

    def test_02_shared_dict_identity_and_bidirectional_mutation(self):
        """공유 dict 객체 Identity 및 양방향 참조 동기화 검증"""
        # 1. Identity 검증
        self.assertIs(simulator.house_states, engine.state.house_states)
        self.assertIs(simulator.device_states, engine.state.device_states)
        self.assertIs(simulator.DEVICE_PROFILES, engine.profiles.DEVICE_PROFILES)

        # 2. simulator.device_states를 통한 수정이 engine.power_model에 반영되는지 확인
        simulator.init_simulation_states(["H001"])
        simulator.device_states["H001"]["kettle"]["state"] = "RUNNING"
        simulator.device_states["H001"]["kettle"]["manual_hold"] = True
        simulator.device_states["H001"]["kettle"]["nominal_w"] = 1650.0
        simulator.device_states["H001"]["kettle"]["nominal_pf"] = 0.99

        p, q, is_active = engine.power_model.update_and_generate_device_load("H001", "kettle", allow_random=False)
        self.assertTrue(is_active)
        self.assertGreater(p, 1600.0)

        # 3. engine.state를 통한 수정이 simulator.device_states에 즉시 반영되는지 확인
        engine.state.set_manual_device_state("H001", "kettle", False)
        self.assertEqual(simulator.device_states["H001"]["kettle"]["state"], "OFF")
        self.assertFalse(simulator.device_states["H001"]["kettle"]["manual_hold"])

    def test_03_initial_state_all_off(self):
        """초기화 후 모든 가전이 OFF 상태인지 검증"""
        simulator.init_simulation_states(["H001"])
        house_devs = simulator.device_states["H001"]
        self.assertEqual(len(house_devs), len(simulator.DEVICE_PROFILES))
        for dev, state in house_devs.items():
            self.assertEqual(state["state"], "OFF", f"{dev} should be OFF initially")
            self.assertFalse(state["manual_hold"])
            self.assertEqual(state["inrush_remaining"], 0)
            self.assertEqual(state["session_remaining"], 0)

    def test_04_manual_on_and_idempotency(self):
        """수동 ON 시 STARTING 진입 및 중복 ON 멱등성 검증"""
        simulator.init_simulation_states(["H001"])

        # 1. 수동 ON
        snap1 = simulator.set_manual_device_state("H001", "kettle", True)
        self.assertEqual(snap1["state"], "STARTING")
        self.assertTrue(snap1["manual_hold"])
        self.assertGreater(snap1["nominal_w"], 0.0)
        self.assertGreater(snap1["session_remaining"], 0)

        kettle_st = simulator.device_states["H001"]["kettle"]
        initial_nominal_w = kettle_st["nominal_w"]
        initial_session = kettle_st["session_remaining"]

        # 2. 중복 ON 호출 시 물리 진행 파라미터가 초기화되지 않는지 확인
        snap2 = simulator.set_manual_device_state("H001", "kettle", True)
        self.assertEqual(snap2["state"], "STARTING")
        self.assertTrue(snap2["manual_hold"])
        self.assertEqual(snap2["nominal_w"], initial_nominal_w)
        self.assertEqual(snap2["session_remaining"], initial_session)

        # 3. 1 tick 진행 (STARTING -> RUNNING 전환 시점)
        simulator.calculate_main_panel_metrics("H001", allow_random=False)
        self.assertEqual(kettle_st["state"], "RUNNING")
        self.assertTrue(kettle_st["manual_hold"])

        # 4. RUNNING 상태에서 다시 중복 ON 호출
        snap3 = simulator.set_manual_device_state("H001", "kettle", True)
        self.assertEqual(snap3["state"], "RUNNING")
        self.assertTrue(snap3["manual_hold"])
        self.assertEqual(snap3["nominal_w"], initial_nominal_w)

    def test_05_manual_off_cleanup(self):
        """수동 OFF 시 상태 및 카운터 완전 초기화 검증"""
        simulator.init_simulation_states(["H001"])
        simulator.set_manual_device_state("H001", "induction", True)
        simulator.calculate_main_panel_metrics("H001", allow_random=False)

        # OFF 호출
        snap_off = simulator.set_manual_device_state("H001", "induction", False)
        self.assertEqual(snap_off["state"], "OFF")
        self.assertFalse(snap_off["manual_hold"])
        self.assertEqual(snap_off["nominal_w"], 0.0)
        self.assertEqual(snap_off["nominal_pf"], 0.0)
        self.assertEqual(snap_off["session_remaining"], 0)
        self.assertEqual(snap_off["inrush_remaining"], 0)
        self.assertFalse(snap_off["is_heating"])
        self.assertEqual(snap_off["duty_remaining"], 0)

    def test_06_allow_random_false_invariant(self):
        """allow_random=False 일 때 가전이 임의로 켜지지 않는 불변식 검증"""
        simulator.init_simulation_states(["H001"])
        for _ in range(50):
            m = simulator.calculate_main_panel_metrics("H001", allow_random=False)
            self.assertEqual(m["active_devices"], [])
            for dev, st in simulator.device_states["H001"].items():
                self.assertEqual(st["state"], "OFF")

    def test_07_metrics_schema_and_types(self):
        """calculate_main_panel_metrics 반환 필드 구조 및 타입 검증"""
        simulator.init_simulation_states(["H001"])
        m = simulator.calculate_main_panel_metrics("H001", allow_random=False)

        expected_fields = [
            "active_power",
            "reactive_power",
            "apparent_power",
            "power_factor",
            "voltage",
            "current",
            "active_devices",
        ]
        for field in expected_fields:
            self.assertIn(field, m)

        self.assertIsInstance(m["active_power"], float)
        self.assertIsInstance(m["reactive_power"], float)
        self.assertIsInstance(m["apparent_power"], float)
        self.assertIsInstance(m["power_factor"], float)
        self.assertIsInstance(m["voltage"], float)
        self.assertIsInstance(m["current"], float)
        self.assertIsInstance(m["active_devices"], list)

    def test_08_induction_duty_cycle_progression(self):
        """인덕션 듀티 사이클의 고출력 -> 휴지 -> 재가열 전이 검증"""
        simulator.init_simulation_states(["H001"])
        simulator.set_manual_device_state("H001", "induction", True)

        phase = "WAIT_HEATING"
        ticks = 0
        max_ticks = 100

        while ticks < max_ticks:
            ticks += 1
            m = simulator.calculate_main_panel_metrics("H001", allow_random=False)
            st = simulator.device_states["H001"]["induction"]

            if phase == "WAIT_HEATING":
                if st["state"] == "RUNNING" and m["active_power"] > 1000.0:
                    phase = "WAIT_PAUSE"
                    self.assertIn("인덕션(전기레인지)", m["active_devices"])
            elif phase == "WAIT_PAUSE":
                if st["state"] == "RUNNING" and m["active_power"] < 300.0:
                    phase = "WAIT_REHEAT"
                    self.assertIn("인덕션(전기레인지)", m["active_devices"])
            elif phase == "WAIT_REHEAT":
                if st["state"] == "RUNNING" and m["active_power"] > 1000.0:
                    phase = "COMPLETED"
                    self.assertIn("인덕션(전기레인지)", m["active_devices"])
                    break

        self.assertEqual(phase, "COMPLETED", f"Duty cycle did not complete full loop within {max_ticks} ticks")

    def test_09_fixed_seed_baseline_exact_match(self):
        """고정 seed 42 baseline fixture와 재귀적 전수 비교 검증

        fixture는 리팩터링 전(bcc5bdd) 출력을 기준으로 만들었고, 2026-09-23 세션 가동 전력을
        nominal_w 범위 난수 → median_w(EDA 실측 중앙값) 고정으로 바꾼 뒤 같은 스텝 시퀀스로 재생성했다.
        """
        fixture_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "simulator_seed42_baseline.json")
        if not os.path.isfile(fixture_path):
            raise FileNotFoundError(f"Fixture 파일을 찾을 수 없습니다: {fixture_path}")

        with open(fixture_path, "r", encoding="utf-8") as f:
            fixture_doc = json.load(f)

        metadata = fixture_doc.get("metadata", {})
        self.assertEqual(metadata.get("source_commit"), "922cbb3+median_w")
        self.assertEqual(metadata.get("random_seed"), 42)
        baseline_steps = fixture_doc["steps"]

        # 리팩터링 후 코드에서 동일 시나리오 재생성
        random.seed(42)
        house = "H001"
        replay_steps = []

        # 1. 초기화
        simulator.init_simulation_states([house])
        replay_steps.append({
            "step_name": "init",
            "house_states": copy.deepcopy(simulator.house_states),
            "device_states": copy.deepcopy(simulator.device_states)
        })

        # 2. 대기전력 5 tick
        for i in range(1, 6):
            m = simulator.calculate_main_panel_metrics(house, allow_random=False)
            replay_steps.append({
                "step_name": f"standby_tick_{i}",
                "metrics": copy.deepcopy(m),
                "house_states": copy.deepcopy(simulator.house_states),
                "device_states": copy.deepcopy(simulator.device_states)
            })

        # 3. kettle 수동 ON
        kettle_on_resp = simulator.set_manual_device_state(house, "kettle", True)
        replay_steps.append({
            "step_name": "kettle_on",
            "resp": copy.deepcopy(kettle_on_resp),
            "house_states": copy.deepcopy(simulator.house_states),
            "device_states": copy.deepcopy(simulator.device_states)
        })

        # 4. kettle 중복 ON
        kettle_dup_resp = simulator.set_manual_device_state(house, "kettle", True)
        replay_steps.append({
            "step_name": "kettle_dup_on",
            "resp": copy.deepcopy(kettle_dup_resp),
            "house_states": copy.deepcopy(simulator.house_states),
            "device_states": copy.deepcopy(simulator.device_states)
        })

        # 5. 가동 중 5 tick
        for i in range(1, 6):
            m = simulator.calculate_main_panel_metrics(house, allow_random=False)
            replay_steps.append({
                "step_name": f"kettle_tick_{i}",
                "metrics": copy.deepcopy(m),
                "house_states": copy.deepcopy(simulator.house_states),
                "device_states": copy.deepcopy(simulator.device_states)
            })

        # 6. kettle OFF
        kettle_off_resp = simulator.set_manual_device_state(house, "kettle", False)
        replay_steps.append({
            "step_name": "kettle_off",
            "resp": copy.deepcopy(kettle_off_resp),
            "house_states": copy.deepcopy(simulator.house_states),
            "device_states": copy.deepcopy(simulator.device_states)
        })

        # 7. induction 수동 ON
        ind_on_resp = simulator.set_manual_device_state(house, "induction", True)
        replay_steps.append({
            "step_name": "induction_on",
            "resp": copy.deepcopy(ind_on_resp),
            "house_states": copy.deepcopy(simulator.house_states),
            "device_states": copy.deepcopy(simulator.device_states)
        })

        # 8. induction 완주
        phase = "WAIT_START"
        ind_ticks = 0
        while ind_ticks < 100:
            ind_ticks += 1
            m = simulator.calculate_main_panel_metrics(house, allow_random=False)
            dev_st = simulator.device_states[house]["induction"]
            state = dev_st["state"]
            is_heating = dev_st["is_heating"]

            replay_steps.append({
                "step_name": f"induction_tick_{ind_ticks}",
                "metrics": copy.deepcopy(m),
                "house_states": copy.deepcopy(simulator.house_states),
                "device_states": copy.deepcopy(simulator.device_states)
            })

            if phase == "WAIT_START":
                if state == "RUNNING" and is_heating:
                    phase = "WAIT_PAUSE"
            elif phase == "WAIT_PAUSE":
                if state == "RUNNING" and not is_heating:
                    phase = "WAIT_REHEAT"
            elif phase == "WAIT_REHEAT":
                if state == "RUNNING" and is_heating:
                    phase = "COMPLETED"
                    break

        replay_steps.append({
            "step_name": "induction_duty_cycle_completed",
            "final_phase": phase,
            "total_induction_ticks": ind_ticks
        })

        # 직렬화 후 비교 (JSON 호환 타입 정규화)
        replay_serialized = serialize_val(replay_steps)

        # 스텝 수 검증
        self.assertEqual(
            len(replay_serialized), len(baseline_steps),
            f"Step count mismatch: {len(replay_serialized)} vs {len(baseline_steps)}"
        )

        # 각 스텝별 데이터 재귀적 전수 비교 (metrics, resp, house_states, device_states, final_phase, total_induction_ticks)
        for idx, (b_step, r_step) in enumerate(zip(baseline_steps, replay_serialized)):
            self._assert_deep_equal(b_step, r_step, path=f"step[{idx}]:{b_step.get('step_name')}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
