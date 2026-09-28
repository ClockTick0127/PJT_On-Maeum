    // ----------------------------------------------------
    // 가구 설정 테이블 동적 렌더링 및 프리셋 관리
    // ----------------------------------------------------
    function renderHouseholdTable() {
      const tbody = document.getElementById('householdTableBody');
      if (!tbody) return;
      tbody.replaceChildren();

      const defaultScenarios = {
        H001: 'peak',
        H002: 'routine_missed',
        H003: 'random',
        H004: 'manual'
      };

      DEFAULT_HOUSES.forEach(house => {
        const tr = document.createElement('tr');
        tr.id = `row_${house}`;

        // 1. 실행 체크박스 (H001 기본 체크)
        const tdChk = document.createElement('td');
        tdChk.style.textAlign = 'center';
        const chk = document.createElement('input');
        chk.type = 'checkbox';
        chk.id = `chk_${house}`;
        // 네이티브 체크박스를 집 아이콘으로 대체한다(.house-run-toggle). 값 자체는 checked 그대로다.
        chk.className = 'house-run-toggle';
        chk.title = `${house} 실행 여부`;
        chk.setAttribute('aria-label', `${house} 실행`);
        chk.checked = (house === 'H001');
        tdChk.appendChild(chk);
        tr.appendChild(tdChk);

        // 2. 가구 ID
        const tdHouse = document.createElement('td');
        tdHouse.textContent = house;
        tdHouse.style.fontWeight = '700';
        tdHouse.style.color = '#b8490d';
        tr.appendChild(tdHouse);

        // 3. 시나리오 선택
        const tdScenario = document.createElement('td');
        const sel = document.createElement('select');
        sel.id = `scenario_${house}`;
        sel.className = 'speed-select';
        sel.style.padding = '3px 8px';
        sel.style.fontSize = '0.8rem';
        const scenariosList = [
          ...(house === 'H001' ? [{ val: 'normal_routine', text: 'normal_routine (H001 정상 일상 루틴)' }] : []),
          { val: 'peak', text: 'peak (피크 부하 시연)' },
          { val: 'routine_missed', text: 'routine_missed (루틴 누락 시연)' },
          { val: 'sensor_fault', text: 'sensor_fault (센서 결측 및 복구)' },
          { val: 'random', text: 'random (랜덤 부하 시뮬레이션)' },
          { val: 'manual', text: 'manual (가전 수동 제어)' }
        ];
        scenariosList.forEach(opt => {
          const optEl = document.createElement('option');
          optEl.value = opt.val;
          optEl.textContent = opt.text;
          sel.appendChild(optEl);
        });
        sel.value = defaultScenarios[house] || 'random';
        tdScenario.appendChild(sel);
        tr.appendChild(tdScenario);

        // 4. 상태 뱃지
        const tdStatus = document.createElement('td');
        const badge = document.createElement('span');
        badge.id = `status_badge_${house}`;
        badge.className = 'badge-hh badge-hh-waiting';
        badge.textContent = '대기';
        tdStatus.appendChild(badge);
        tr.appendChild(tdStatus);

        // 5. 최신 전력
        const tdPower = document.createElement('td');
        tdPower.id = `power_val_${house}`;
        tdPower.style.fontFamily = 'monospace';
        tdPower.textContent = '-';
        tr.appendChild(tdPower);

        tbody.appendChild(tr);
      });
    }

    function applyPreset(type) {
      DEFAULT_HOUSES.forEach(house => {
        const chk = document.getElementById(`chk_${house}`);
        const sel = document.getElementById(`scenario_${house}`);
        if (!chk || !sel) return;

        if (type === 'normal_single') {
          chk.checked = (house === 'H001');
          if (house === 'H001') sel.value = 'normal_routine';
        } else if (type === 'peak_single') {
          chk.checked = (house === 'H001');
          if (house === 'H001') sel.value = 'peak';
        } else if (type === 'missed_single') {
          chk.checked = (house === 'H001');
          if (house === 'H001') sel.value = 'routine_missed';
        } else if (type === 'fault_single') {
          chk.checked = (house === 'H001');
          if (house === 'H001') sel.value = 'sensor_fault';
        } else if (type === 'manual_single') {
          chk.checked = (house === 'H001');
          if (house === 'H001') sel.value = 'manual';
        } else if (type === 'comprehensive') {
          chk.checked = ['H001', 'H002', 'H003', 'H004'].includes(house);
          if (house === 'H001') sel.value = 'peak';
          if (house === 'H002') sel.value = 'routine_missed';
          if (house === 'H003') sel.value = 'random';
          if (house === 'H004') sel.value = 'manual';
        } else if (type === 'all_random') {
          chk.checked = true;
          sel.value = 'random';
        }
      });
    }

    function setTableAndControlsEnabled(enabled) {
      isControlsEnabled = enabled;
      DEFAULT_HOUSES.forEach(house => {
        const chk = document.getElementById(`chk_${house}`);
        const sel = document.getElementById(`scenario_${house}`);
        if (chk) chk.disabled = !enabled;
        if (sel) sel.disabled = !enabled;
      });
      const btnStart = document.getElementById('btnStartSimulation');
      if (btnStart) btnStart.disabled = !enabled;

      ['btnPresetPeak', 'btnPresetMissed', 'btnPresetFault', 'btnPresetManual', 'btnPresetMulti', 'btnPresetAllRandom'].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) btn.disabled = !enabled;
      });

      const btnDemoNormal = document.getElementById('btnDemoNormalRoutine');
      if (btnDemoNormal) btnDemoNormal.disabled = !enabled;
      const btnDemoMissed = document.getElementById('btnDemoRoutineMissed');
      if (btnDemoMissed) btnDemoMissed.disabled = !enabled;
      const btnDemoFault = document.getElementById('btnDemoSensorFault');
      if (btnDemoFault) btnDemoFault.disabled = !enabled;

      const faultDurInput = document.getElementById('faultDurationInput');
      if (faultDurInput) faultDurInput.disabled = !enabled;
      const seedInput = document.getElementById('seedInput');
      if (seedInput) seedInput.disabled = !enabled;
      const startTimeInput = document.getElementById('startTimeInput');
      if (startTimeInput) startTimeInput.disabled = !enabled;

      setSimulationDateInputEnabled(enabled);
    }

    // 관찰 가구 드롭다운을 H001 ~ H010 전체로 재구성 (설정된 가구는 시나리오 라벨 병기)
    function rebuildObservedHouseSelect(configured) {
      const select = document.getElementById('observedHouseSelect');
      if (!select) return;
      const items = Array.isArray(configured) ? configured : [];
      const scenarioByHouse = new Map(items.map(item => [item.house, item.scenario]));
      const houses = [...DEFAULT_HOUSES];
      items.forEach(item => {
        if (!houses.includes(item.house)) houses.push(item.house);
      });

      select.replaceChildren();
      houses.forEach(house => {
        const opt = document.createElement('option');
        opt.value = house;
        const scenario = scenarioByHouse.get(house);
        opt.textContent = scenario ? `${house} (${scenario})` : house;
        select.appendChild(opt);
      });

      // 설정된 가구가 있으면 관찰 대상을 그중 하나로 맞추고, 없으면 기존 선택을 유지
      if (items.length > 0 && !scenarioByHouse.has(observedHouse)) {
        observedHouse = items[0].house;
      } else if (!houses.includes(observedHouse)) {
        observedHouse = houses[0];
      }
      select.value = observedHouse;
      const badge = document.getElementById('observedHouseBadge');
      if (badge) badge.textContent = observedHouse;
    }

    function ensureHouseInObservedSelect(house) {
      const select = document.getElementById('observedHouseSelect');
      if (!select) return;
      const options = select.options || select.children || [];
      let found = false;
      for (let i = 0; i < options.length; i++) {
        if (options[i].value === house) {
          found = true;
          break;
        }
      }
      if (!found) {
        const opt = document.createElement('option');
        opt.value = house;
        opt.textContent = house;
        select.appendChild(opt);
      }
    }

    function updateHouseholdTableRow(house, status, sec, totalP, isFault = false, gapElapsed = 0, faultDuration = 120) {
      const badge = document.getElementById(`status_badge_${house}`);
      if (badge) {
        if (isFault) {
          badge.className = 'badge-hh badge-hh-fault';
          badge.textContent = `센서고장 (${gapElapsed ?? 0}/${faultDuration ?? 120}초)`;
        } else if (status === 'completed') {
          badge.className = 'badge-hh badge-hh-completed';
          badge.textContent = `완료 (${sec}s)`;
        } else if (status === 'running') {
          badge.className = 'badge-hh badge-hh-running';
          badge.textContent = `실행중 (${sec}s)`;
        } else if (status === 'stopped') {
          badge.className = 'badge-hh badge-hh-stopped';
          badge.textContent = '정지됨';
        } else {
          badge.className = 'badge-hh badge-hh-waiting';
          badge.textContent = '대기';
        }
      }
      const pCell = document.getElementById(`power_val_${house}`);
      if (pCell) {
        if (isFault || totalP === null || totalP === undefined) {
          pCell.textContent = '—';
        } else if (typeof totalP === 'number') {
          pCell.textContent = `${totalP.toFixed(1)} W`;
        }
      }
    }

