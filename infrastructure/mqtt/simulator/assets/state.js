    // ==========================================
    // 1. 물리 엔진 및 6대 가전 프로파일 정의 (simulator.py와 100% 동일)
    // ==========================================
    const DEVICE_PROFILES = {
      kettle: { name_ko: "전기포트", nominal_w: [1500, 1800], median_w: 1657, inrush_factor: 1.02, pf_nominal: [0.98, 1.00] },
      induction: { name_ko: "인덕션", nominal_w: [1300, 1750], median_w: 1463, inrush_factor: 1.08, pf_nominal: [0.91, 0.95] },
      iron: { name_ko: "전기다리미", nominal_w: [1200, 1550], median_w: 1389, inrush_factor: 1.02, pf_nominal: [0.98, 1.00] },
      microwave: { name_ko: "전자레인지", nominal_w: [850, 1150], median_w: 941, inrush_factor: 1.35, pf_nominal: [0.88, 0.94] },
      hair_dryer: { name_ko: "헤어드라이기", nominal_w: [800, 1200], median_w: 934, inrush_factor: 1.20, pf_nominal: [0.94, 0.98] },
      vacuum_cleaner: { name_ko: "진공청소기", nominal_w: [700, 950], median_w: 819, inrush_factor: 1.50, pf_nominal: [0.75, 0.85] }
    };

    // 다중 가구 지원 상태 관리
    const DEFAULT_HOUSES = ["H001", "H002", "H003", "H004", "H005", "H006", "H007", "H008", "H009", "H010"];
    const latestMetricsByHouse = new Map(); // house -> 최신 SSE metrics 객체
    const historyByHouse = new Map();       // house -> { times: [], activeP: [], ... }
    const maxPowerByHouse = new Map();      // house -> 최고 소비전력 기록 (가구별 독립 관리)
    let observedHouse = "H001";             // 현재 대시보드에서 관찰 및 차트 렌더링할 대상 가구
    let activeConfiguredHouseholds = [];    // 현재 실행 시작된 가구 목록

    function getOrCreateHistory(house) {
      if (!historyByHouse.has(house)) {
        historyByHouse.set(house, {
          times: [], activeP: [], reactiveP: [], apparentS: [],
          currents: [], pfs: [], voltages: [], activeLabels: []
        });
      }
      return historyByHouse.get(house);
    }

    // 상태 관리 (로컬/단일 호환)
    let houseEnv = { voltage: 220.0, base_p: 55.0, fridge_p: 0.0, fridge_active: false, fridge_timer: 60 };
    let deviceStates = {};
    function initDevices() {
      Object.keys(DEVICE_PROFILES).forEach(dev => {
        deviceStates[dev] = { state: "OFF", p: 0, pf: 0.95, session_remaining: 0, nominal_w: 0, nominal_pf: 0 };
      });
    }
    initDevices();

    // 시뮬레이션 제어 변수
    let timerId = null;
    let currentMode = null; // 'peak', 'routine_missed', 'random', 'manual', 'multi'
    let currentSec = 0;
    let intervalMs = 1000;
    let isPaused = false;
    let maxPowerRecorded = 0.0;
    let isControlsEnabled = true;
    let isSimulationRunning = false;
    let lastSuccessfulSpeedMs = 1000;
    let isSpeedUpdating = false;

    // 수동 제어 모드 및 디바이스 상태 관리 (Map 기반 pending 추적)
    let latestDeviceStates = {};
    const pendingDeviceRequests = new Map(); // device -> { enabled: boolean, timeoutId: number }
    let hasReceivedManualSse = false;
    const PENDING_TIMEOUT_MS = 5000;

