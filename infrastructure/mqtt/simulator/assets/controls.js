    // ==========================================
    // 5. 버튼 컨트롤 핸들러 및 날짜 헬퍼 (서버 연동 지원)
    // ==========================================
    function getSimulationDateTimeLabel(m) {
      if (!m) return '';
      if (m.simDateTimeKst) {
        const s = String(m.simDateTimeKst).trim();
        return s.endsWith('KST') ? `[${s}] ` : `[${s} KST] `;
      }
      if (m.simTimeKst) {
        const s = String(m.simTimeKst).trim();
        return s.endsWith('KST') ? `[${s}] ` : `[${s} KST] `;
      }
      return '';
    }

    function getKstTodayDateString() {
      const formatter = new Intl.DateTimeFormat('ko-KR', {
        timeZone: 'Asia/Seoul',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      });
      const parts = formatter.formatToParts(new Date());
      const year = parts.find(p => p.type === 'year').value;
      const month = parts.find(p => p.type === 'month').value;
      const day = parts.find(p => p.type === 'day').value;
      return `${year}-${month}-${day}`;
    }

    function setSimulationDateInputEnabled(enabled) {
      const input = document.getElementById('simDateInput');
      if (input) {
        input.disabled = !enabled;
      }
    }

    // Seed 입력값 파싱: 빈칸이면 서버 자동 생성(seed: null), 그 외에는 0~2^31-1 정수만 허용
    const SEED_MAX = 2147483647;
    let currentSeed = null;

    function parseSeedInput() {
      const el = document.getElementById('seedInput');
      const raw = (el && typeof el.value === 'string') ? el.value.trim() : '';
      if (!raw) return { ok: true, seed: null };
      if (!/^\d+$/.test(raw)) return { ok: false, seed: null };
      const parsed = Number(raw);
      if (!Number.isSafeInteger(parsed) || parsed > SEED_MAX) return { ok: false, seed: null };
      return { ok: true, seed: parsed };
    }

    // 현재 실행 seed 표시 (재현하려면 이 값을 Seed 입력칸에 넣고 같은 가구/시나리오로 다시 시작)
    function setCurrentSeed(seed) {
      currentSeed = (typeof seed === 'number' && Number.isInteger(seed)) ? seed : null;
      const label = document.getElementById('currentSeedLabel');
      if (label) label.textContent = `현재 seed: ${currentSeed === null ? '-' : currentSeed}`;
      const btn = document.getElementById('btnCopySeed');
      if (btn) btn.disabled = (currentSeed === null);
    }

    async function copyCurrentSeed() {
      if (currentSeed === null) return;
      try {
        await navigator.clipboard.writeText(String(currentSeed));
      } catch (_) {
        // 클립보드 권한이 없으면 입력칸에 채워 두어 수동 복사/재사용이 가능하게 한다.
        const el = document.getElementById('seedInput');
        if (el) el.value = String(currentSeed);
      }
    }

    // 다중 가구 통합 시뮬레이션 시작 핸들러
    async function startMultiSimulation() {
      const selected = [];
      DEFAULT_HOUSES.forEach(house => {
        const chk = document.getElementById(`chk_${house}`);
        const sel = document.getElementById(`scenario_${house}`);
        if (chk && chk.checked) {
          selected.push({
            house: house,
            scenario: sel ? sel.value : 'random'
          });
        }
      });

      if (selected.length === 0) {
        showNoticeError("최소 1개 이상의 가구를 선택해야 시뮬레이션을 시작할 수 있습니다.");
        return;
      }

      const dateInput = document.getElementById('simDateInput');
      const simDate = dateInput ? dateInput.value : '';
      if (simDate) {
        const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
        if (!dateRegex.test(simDate)) {
          showNoticeError("올바르지 않은 날짜 형식입니다. YYYY-MM-DD 형식으로 입력해주세요.");
          return;
        }
      }

      // 선택된 가구 중 sensor_fault 시나리오가 포함되어 있을 때만 faultDurationInput 검증
      const hasSensorFault = selected.some(item => item.scenario === 'sensor_fault');
      let effectiveFaultDuration = null;
      if (hasSensorFault) {
        const faultDurationEl = document.getElementById('faultDurationInput');
        const rawVal = faultDurationEl ? faultDurationEl.value.trim() : '';
        const parsed = parseInt(rawVal, 10);
        if (!rawVal || isNaN(parsed) || parsed < 1 || parsed > 3600 || String(parsed) !== rawVal) {
          showNoticeError("결측 시간은 1~3600 사이의 정수(초)여야 합니다.");
          return;
        }
        effectiveFaultDuration = parsed;
      }

      const startTimeEl = document.getElementById('startTimeInput');
      const startTime = (startTimeEl && typeof startTimeEl.value === 'string') ? startTimeEl.value.trim() : '';
      if (startTime) {
        if (!/^\d{2}:\d{2}(:\d{2})?$/.test(startTime)) {
          showNoticeError("시작 시각은 HH:MM 또는 HH:MM:SS 형식이어야 합니다.");
          return;
        }
        if (selected.some(item => item.scenario === 'normal_routine' || item.scenario === 'routine_missed')) {
          showNoticeError("정상 루틴/루틴 누락 시나리오는 시작 시각이 고정되어 있어 시작 시각을 지정할 수 없습니다. 시작 시각 칸을 비워 주세요.");
          return;
        }
      }

      const seedParse = parseSeedInput();
      if (!seedParse.ok) {
        showNoticeError("Seed는 0~2147483647 사이의 정수여야 합니다. 비워 두면 자동 생성됩니다.");
        return;
      }
      const seedValue = seedParse.seed;

      if (!isServerConnected) {
        showNoticeError("백엔드 서버에 연결되어 있지 않습니다. 다중 가구 시뮬레이션은 서버 실행 상태에서만 가능합니다.");
        return;
      }

      const resetOk = await resetSimulation();
      if (!resetOk) {
        return;
      }

      const speedSelectEl = document.getElementById('speedSelect');
      const selectedMs = speedSelectEl ? parseInt(speedSelectEl.value, 10) : intervalMs;
      const targetInterval = (selectedMs && !isNaN(selectedMs)) ? (selectedMs / 1000.0) : (intervalMs / 1000.0);
      intervalMs = (selectedMs && !isNaN(selectedMs)) ? selectedMs : intervalMs;
      lastSuccessfulSpeedMs = intervalMs;

      const payload = {
        households: selected,
        interval: targetInterval
      };
      if (seedValue !== null) {
        payload.seed = seedValue;
      }
      if (startTime) {
        payload.start_time = startTime;
      }
      if (simDate && simDate.trim()) {
        payload.simulation_date = simDate.trim();
      }
      if (effectiveFaultDuration !== null) {
        payload.fault_duration_sec = effectiveFaultDuration;
      }

      currentMode = (selected.length === 1) ? selected[0].scenario : 'multi';
      activeConfiguredHouseholds = selected;

      // 관찰 가구 드롭다운 구성 (H001 ~ H010 전체 유지)
      rebuildObservedHouseSelect(selected);

      isPaused = false;
      clearAllPendingRequests();
      setTableAndControlsEnabled(false);
      document.getElementById('btnPause').disabled = false;
      document.getElementById('btnPause').textContent = "일시정지";
      document.getElementById('badgeStatus').className = "badge-status status-running";
      document.getElementById('badgeStatus').textContent = `시뮬레이션 실행 중 (${selected.length}개 가구)`;
      // 실시간 흐름 표현으로 전환 (직전 BURST 진행 표시가 남아 있더라도 해제)
      powerflowSetMode('live');
      powerflowSetRunState('running');

      selected.forEach(item => {
        const badge = document.getElementById(`status_badge_${item.house}`);
        if (badge) {
          badge.className = 'badge-hh badge-hh-running';
          badge.textContent = '실행중';
        }
      });

      try {
        const res = await fetch('api/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        if (!res.ok) {
          isSimulationRunning = false;
          let errMsg = `시작 실패 (${res.status})`;
          try {
            const errData = await res.json();
            if (errData && errData.message) errMsg = errData.message;
          } catch (_) { }
          showNoticeError(`시뮬레이션 시작 오류: ${errMsg}`);
          setTableAndControlsEnabled(true);
          document.getElementById('btnPause').disabled = true;
          document.getElementById('badgeStatus').className = "badge-status status-normal";
          document.getElementById('badgeStatus').textContent = "시작 실패";
          powerflowSetRunState('idle');
          return;
        }

        isSimulationRunning = true;
        try {
          const startedData = await res.json();
          setCurrentSeed(startedData ? startedData.seed : null);
        } catch (_) { }
        connectServerStream();
      } catch (err) {
        isSimulationRunning = false;
        showNoticeError(`시작 요청 네트워크 오류: ${err.message || err}`);
        setTableAndControlsEnabled(true);
        document.getElementById('btnPause').disabled = true;
      }
    }

    // 발표용 원클릭 시연 제어 함수
    async function startNormalRoutineDemo() {
      applyPreset('normal_single');
      await startMultiSimulation();
    }

    async function startRoutineMissedDemo() {
      applyPreset('missed_single');
      await startMultiSimulation();
    }

    async function startSensorFaultDemo() {
      applyPreset('fault_single');
      await startMultiSimulation();
    }

    // 기존 단일 실행 프리셋 래퍼 (하위 호환)
    function startPeakDemo() {
      applyPreset('peak_single');
      startMultiSimulation();
    }

    function startRandomSimulation() {
      applyPreset('all_random');
      startMultiSimulation();
    }

    function startManualDemo() {
      applyPreset('manual_single');
      startMultiSimulation();
    }

    async function togglePause() {
      if (!isServerConnected) return;

      if (!isPaused) {
        try {
          const res = await fetch('api/pause', { method: 'POST' });
          if (!res.ok) {
            let errMsg = `일시정지 실패 (${res.status})`;
            try {
              const errData = await res.json();
              if (errData && errData.message) errMsg = errData.message;
            } catch (_) { }
            showNoticeError(errMsg);
            return;
          }
        } catch (err) {
          showNoticeError(`일시정지 네트워크 오류: ${err.message || err}`);
          return;
        }

        isPaused = true;
        clearAllPendingRequests();
        updateApplianceButtonsState();
        document.getElementById('btnPause').textContent = "재생";
        document.getElementById('badgeStatus').textContent = "일시정지됨";
        powerflowSetRunState('paused');
      } else {
        try {
          const res = await fetch('api/resume', { method: 'POST' });
          if (!res.ok) {
            let errMsg = `재개 실패 (${res.status})`;
            try {
              const errData = await res.json();
              if (errData && errData.message) errMsg = errData.message;
            } catch (_) { }
            showNoticeError(errMsg);
            return;
          }
        } catch (err) {
          showNoticeError(`재개 네트워크 오류: ${err.message || err}`);
          return;
        }

        isPaused = false;
        clearAllPendingRequests();
        updateApplianceButtonsState();
        document.getElementById('btnPause').textContent = "일시정지";
        document.getElementById('badgeStatus').textContent = "시뮬레이션 실행 중";
        powerflowSetRunState('running');
      }
    }

    async function stopSimulation() {
      const wasRunning = isSimulationRunning;

      if (isServerConnected) {
        try {
          const res = await fetch('api/stop', { method: 'POST' });
          if (!res.ok) {
            isSimulationRunning = wasRunning;
            let errMsg = `정지 실패 (${res.status})`;
            try {
              const errData = await res.json();
              if (errData && errData.message) errMsg = errData.message;
            } catch (_) { }
            showNoticeError(`시뮬레이터 정지 오류: ${errMsg}`);
            return false;
          }
        } catch (err) {
          isSimulationRunning = wasRunning;
          showNoticeError(`정지 요청 네트워크 오류: ${err.message || err}`);
          return false;
        }

        // 서버 Stop 성공
        isSimulationRunning = false;
        powerflowSetRunState('idle');
        if (evtSource) { evtSource.close(); evtSource = null; }
        setTableAndControlsEnabled(true);
        const pauseBtn = document.getElementById('btnPause');
        if (pauseBtn) pauseBtn.disabled = true;
        return true;
      }

      // 브라우저 로컬 모드
      if (timerId) {
        clearInterval(timerId);
        timerId = null;
      }
      isSimulationRunning = false;
      powerflowSetRunState('idle');
      setTableAndControlsEnabled(true);
      const pauseBtn = document.getElementById('btnPause');
      if (pauseBtn) pauseBtn.disabled = true;
      return true;
    }

    async function resetSimulation() {
      const wasRunning = isSimulationRunning;

      if (isServerConnected) {
        try {
          const res = await fetch('api/reset', { method: 'POST' });
          if (!res.ok) {
            isSimulationRunning = wasRunning;
            let errMsg = `초기화 실패 (${res.status})`;
            try {
              const errData = await res.json();
              if (errData && errData.message) errMsg = errData.message;
            } catch (_) { }
            showNoticeError(`시뮬레이터 초기화 오류: ${errMsg}`);
            return false;
          }
        } catch (err) {
          isSimulationRunning = wasRunning;
          showNoticeError(`초기화 요청 네트워크 오류: ${err.message || err}`);
          return false;
        }

        // reset 성공 후에만 기존 SSE 연결을 닫는다.
        if (evtSource) { evtSource.close(); evtSource = null; }
      }

      // 서버 Reset 성공 또는 로컬 Reset인 경우에만 false로 전이
      isSimulationRunning = false;

      if (timerId) {
        clearInterval(timerId);
        timerId = null;
      }
      currentSec = 0;
      isPaused = false;
      maxPowerRecorded = 0.0;
      maxPowerByHouse.clear();
      initDevices();

      // 버퍼 초기화
      latestMetricsByHouse.clear();
      historyByHouse.clear();
      activeConfiguredHouseholds = [];

      // 차트 초기화
      chartMain.data.labels = [];
      chartMain.data.datasets.forEach(ds => ds.data = []);
      chartMain.update();

      chartSecondary.data.labels = [];
      chartSecondary.data.datasets.forEach(ds => ds.data = []);
      chartSecondary.update();

      // UI 초기화
      document.getElementById('valPower').textContent = "0.0";
      document.getElementById('valApparent').textContent = "0.0";
      document.getElementById('valCurrent').textContent = "0.00";
      document.getElementById('valVoltage').textContent = "220.0";
      document.getElementById('valPf').textContent = "1.000";
      document.getElementById('valReactive').textContent = "0.0";
      document.getElementById('valTime').textContent = "T+00s";
      document.getElementById('valMaxPower').textContent = "0.0 W";
      document.getElementById('badgeStatus').className = "badge-status status-normal";
      document.getElementById('badgeStatus').textContent = "대기 중";
      document.getElementById('cardPower').className = "stat-card";
      document.getElementById('btnPause').disabled = true;
      document.getElementById('btnPause').textContent = "일시정지";

      currentMode = null;
      // 흐름 패널도 초기 상태(실시간 흐름 모드 · 대기)로 되돌린다.
      powerflowReset();
      latestDeviceStates = {};
      clearAllPendingRequests();
      hasReceivedManualSse = false;

      // 테이블 뱃지 및 전력 초기화
      DEFAULT_HOUSES.forEach(house => {
        const badge = document.getElementById(`status_badge_${house}`);
        if (badge) {
          badge.className = 'badge-hh badge-hh-waiting';
          badge.textContent = '대기';
        }
        const pCell = document.getElementById(`power_val_${house}`);
        if (pCell) pCell.textContent = '-';
      });

      // 가전 칩 초기화
      Object.keys(DEVICE_PROFILES).forEach(dev => {
        const chip = document.getElementById(`chip_${dev}`);
        const stLabel = document.getElementById(`state_${dev}`);
        if (chip) {
          chip.className = "app-chip";
          chip.setAttribute("aria-pressed", "false");
          chip.disabled = true;
        }
        if (stLabel) {
          stLabel.textContent = "OFF";
        }
      });
      updateApplianceButtonsState();

      setTableAndControlsEnabled(true);

      document.getElementById('eventTableBody').innerHTML = '<tr><td colspan="5" style="text-align:center; color:#94a3b8;">시뮬레이션을 시작하면 실시간 이벤트가 기록됩니다.</td></tr>';
      document.getElementById('timelineNotice').innerHTML = '[준비 완료] H001 정상 일상 또는 H001 이상 감지 버튼을 선택하세요.';

      return true;
    }

    async function changeSpeed(val) {
      const speedSelect = document.getElementById('speedSelect');
      const newMs = parseInt(val, 10);
      if (isNaN(newMs) || newMs <= 0) return;

      // 브라우저 로컬 타이머 모드 (서버 미연결이거나 시뮬레이션 미실행 상태)
      if (!isServerConnected || !isSimulationRunning) {
        intervalMs = newMs;
        lastSuccessfulSpeedMs = newMs;
        if (timerId && !isPaused) {
          clearInterval(timerId);
          timerId = setInterval(tick, intervalMs);
        }
        return;
      }

      // 서버 연결 + 시뮬레이션 실행 중 (Pause 상태 포함)
      if (isSpeedUpdating) return; // 요청 직렬화: 중복 요청 방지
      isSpeedUpdating = true;
      if (speedSelect) speedSelect.disabled = true;

      const targetInterval = newMs / 1000.0;
      try {
        const res = await fetch('api/speed', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ interval: targetInterval })
        });

        if (!res.ok) {
          let errMsg = `배속 변경 실패 (${res.status})`;
          try {
            const errData = await res.json();
            if (errData && errData.message) errMsg = errData.message;
          } catch (_) { }
          throw new Error(errMsg);
        }

        intervalMs = newMs;
        lastSuccessfulSpeedMs = newMs;
        if (timerId && !isPaused) {
          clearInterval(timerId);
          timerId = setInterval(tick, intervalMs);
        }
      } catch (err) {
        showNoticeError(`배속 변경 오류: ${err.message || err}`);
        // 마지막 성공값으로 복원
        intervalMs = lastSuccessfulSpeedMs;
        if (speedSelect) {
          speedSelect.value = String(lastSuccessfulSpeedMs);
        }
        if (timerId && !isPaused) {
          clearInterval(timerId);
          timerId = setInterval(tick, intervalMs);
        }
      } finally {
        isSpeedUpdating = false;
        if (speedSelect) speedSelect.disabled = false;
      }
    }

    // 초기 기준 날짜 세팅 (Asia/Seoul 기준 오늘 날짜)
    const dateInputInit = document.getElementById('simDateInput');
    if (dateInputInit && !dateInputInit.value) {
      dateInputInit.value = getKstTodayDateString();
    }

    // 초기 가구 설정 테이블 렌더링
    renderHouseholdTable();

    // 초기 서버 연결 상태 감지
    checkServerConnection();

