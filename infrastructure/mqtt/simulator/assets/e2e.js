    // ==========================================
    // 결정적 E2E 시나리오 실행 제어 패널 로직
    // ==========================================
    let e2eScenarios = [];
    let e2eCurrentRunId = null;
    let e2ePollTimer = null;
    let e2eOverallStatus = 'IDLE';
    let e2ePollIntervalMs = 1000;
    let e2eLastSnapshot = null;

    function e2eIsTerminalStatus(status) {
      return ['COMPLETED', 'STOPPED', 'FAILED', 'PARTIAL_FAILED'].includes(status);
    }

    function e2eShowMessage(msg, isError = true) {
      const area = document.getElementById('e2eMessageArea');
      if (area) {
        area.textContent = msg;
        area.style.display = 'block';
        if (isError) {
          area.style.background = 'rgba(239, 68, 68, 0.2)';
          area.style.border = '1px solid rgba(239, 68, 68, 0.5)';
          area.style.color = '#b91c1c';
        } else {
          area.style.background = 'rgba(243, 121, 41, 0.2)';
          area.style.border = '1px solid rgba(243, 121, 41, 0.5)';
          area.style.color = '#b8490d';
        }
      }
    }

    function e2eClearMessage() {
      const area = document.getElementById('e2eMessageArea');
      if (area) {
        area.textContent = '';
        area.style.display = 'none';
      }
    }

    function e2eSetOverallStatus(status) {
      e2eOverallStatus = status;
      const badge = document.getElementById('e2eOverallStatusBadge');
      if (badge) {
        badge.textContent = status;
        let badgeClass = 'status-normal';
        if (status === 'RUNNING') badgeClass = 'status-running';
        else if (status === 'PAUSED' || status === 'PAUSING') badgeClass = 'status-missed';
        else if (status === 'STOPPED' || status === 'STOPPING') badgeClass = 'badge-hh-stopped';
        else if (status === 'COMPLETED') badgeClass = 'status-normal';
        else if (status === 'FAILED' || status === 'PARTIAL_FAILED') badgeClass = 'status-peak';
        badge.className = `badge-status ${badgeClass}`;
      }

      const isTerminal = e2eIsTerminalStatus(status);
      const isIdle = (status === 'IDLE');
      const isRunningOrActive = (!isIdle && !isTerminal);

      const btnStart = document.getElementById('e2eBtnStart');
      if (btnStart) btnStart.disabled = isRunningOrActive;

      const btnStop = document.getElementById('e2eBtnStop');
      if (btnStop) btnStop.disabled = !isRunningOrActive;

      // 세션 전체 상태는 설정 입력창의 활성화에도 사용 [1]
      const scSel = document.getElementById('e2eScenarioSelect');
      if (scSel) scSel.disabled = isRunningOrActive;
      const hhSel = document.getElementById('e2eHouseholdSelect');
      if (hhSel) hhSel.disabled = isRunningOrActive;
      const refDate = document.getElementById('e2eRefDateInput');
      if (refDate) refDate.disabled = isRunningOrActive;
      const modeSel = document.getElementById('e2eModeSelect');
      if (modeSel) modeSel.disabled = isRunningOrActive;
      const speedIn = document.getElementById('e2eSpeedInput');
      if (speedIn) speedIn.disabled = isRunningOrActive;
    }

    function e2eOnModeChange() {
      const modeSel = document.getElementById('e2eModeSelect');
      const mode = modeSel ? modeSel.value : 'BURST';
      const speedGroup = document.getElementById('e2eSpeedGroup');
      if (speedGroup) {
        speedGroup.style.display = (mode === 'ACCELERATED') ? 'flex' : 'none';
      }
      const noticeBurst = document.getElementById('e2eNoticeBurst');
      if (noticeBurst) {
        noticeBurst.style.display = (mode === 'BURST') ? 'block' : 'none';
      }
    }

    function e2eOnScenarioChange() {
      const scSel = document.getElementById('e2eScenarioSelect');
      const selectedId = scSel ? scSel.value : '';
      const scInfo = e2eScenarios.find(s => s.scenario_id === selectedId);
      const noticeLong = document.getElementById('e2eNoticeLongRun');
      if (noticeLong) {
        if (scInfo && scInfo.total_days >= 20) {
          noticeLong.style.display = 'block';
          noticeLong.textContent = `📅 장기 시나리오(${scInfo.total_days}일): 총 계획 슬롯 수가 약 ${(scInfo.planned_virtual_slots || 0).toLocaleString()}건인 대용량 시나리오입니다.`;
        } else {
          noticeLong.style.display = 'none';
        }
      }
    }

    function e2eOnPollIntervalChange() {
      const sel = document.getElementById('e2ePollIntervalSelect');
      if (sel && sel.value) {
        e2ePollIntervalMs = parseInt(sel.value, 10) || 1000;
        if (e2ePollTimer) {
          clearInterval(e2ePollTimer);
          e2ePollTimer = setInterval(e2ePollStatus, e2ePollIntervalMs);
        }
      }
    }

    async function e2eLoadScenarios() {
      const select = document.getElementById('e2eScenarioSelect');
      if (!select) return;
      try {
        const resp = await fetch('api/e2e/scenarios');
        const data = await resp.json();
        if (data && Array.isArray(data.scenarios)) {
          e2eScenarios = data.scenarios;
          if (select.replaceChildren) {
            select.replaceChildren();
          } else {
            select.innerHTML = '';
          }
          data.scenarios.forEach(sc => {
            const opt = document.createElement('option');
            opt.value = sc.scenario_id;
            opt.setAttribute('value', sc.scenario_id);
            const daysText = `${sc.total_days}일`;
            const samplesText = `${Number(sc.planned_publish_samples).toLocaleString()}건 발행`;
            opt.textContent = `${sc.scenario_id} (${daysText} / ${samplesText})`;
            select.appendChild(opt);
          });
          if (data.scenarios.length > 0) {
            select.value = data.scenarios[0].scenario_id;
          }
          e2eOnScenarioChange();
        }
      } catch (err) {
        console.error("E2E 시나리오 목록 조회 실패:", err);
      }
    }

    async function e2eStartRun() {
      if (typeof isSimulationRunning !== 'undefined' && isSimulationRunning) {
        e2eShowMessage("실시간 레거시 시뮬레이터가 실행 중입니다. 중지 후 E2E를 시작하세요.");
        return;
      }
      if (e2eCurrentRunId && !e2eIsTerminalStatus(e2eOverallStatus)) {
        e2eShowMessage("이미 E2E 실행이 진행 중입니다. 세션을 중지하거나 완료 후 다시 시작하세요.");
        return;
      }

      const scSel = document.getElementById('e2eScenarioSelect');
      const scenario = scSel ? scSel.value : '';
      if (!scenario) {
        e2eShowMessage("시나리오를 선택하세요.");
        return;
      }

      const hhSel = document.getElementById('e2eHouseholdSelect');
      const household = hhSel ? hhSel.value : 'H001';

      const refDateInput = document.getElementById('e2eRefDateInput');
      const refDate = refDateInput ? refDateInput.value : '';
      if (!refDate || !/^\d{4}-\d{2}-\d{2}$/.test(refDate)) {
        e2eShowMessage("올바른 기준일자(YYYY-MM-DD)를 입력하세요.");
        return;
      }

      const modeSel = document.getElementById('e2eModeSelect');
      const mode = modeSel ? modeSel.value : 'BURST';

      let speedVal = undefined;
      if (mode === 'ACCELERATED') {
        const speedInput = document.getElementById('e2eSpeedInput');
        const rawSpeed = speedInput ? speedInput.value : '';
        const numSpeed = parseFloat(rawSpeed);
        if (!rawSpeed || isNaN(numSpeed) || !isFinite(numSpeed) || numSpeed <= 0) {
          e2eShowMessage("ACCELERATED 모드에서는 0보다 큰 유효한 배속(speed)을 입력해야 합니다.");
          return;
        }
        speedVal = numSpeed;
      }

      const payload = {
        reference_date: refDate,
        execution: {
          mode: mode
        },
        households: [
          {
            household_id: household,
            scenario: scenario
          }
        ]
      };
      if (mode === 'ACCELERATED' && speedVal !== undefined) {
        payload.execution.speed = speedVal;
      }

      e2eClearMessage();
      try {
        const resp = await fetch('api/e2e/runs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await resp.json().catch(() => ({}));
        if (resp.status === 202) {
          e2eCurrentRunId = data.run_id;
          powerflowBeginRun(data.run_id);
          const runIdDisplay = document.getElementById('e2eRunIdDisplay');
          if (runIdDisplay) runIdDisplay.textContent = data.run_id;
          e2eSetOverallStatus(data.state || 'STARTING');
          e2eStartPolling();
        } else if (resp.status === 409) {
          // [3] 시작 요청이 409를 받은 경우 오류 배너에 안내문구 노출
          const msg = data.message || "실행 충돌";
          e2eShowMessage(`[409 Conflict] ${msg} — 이미 실행 중인 E2E 세션이 있다. run_id 를 입력해 연결하거나 기존 실행을 중지하라`);
          e2eSetOverallStatus('IDLE');
        } else {
          const msg = data.message || `HTTP ${resp.status}`;
          e2eShowMessage(`실행 시작 실패: ${msg}`);
        }
      } catch (err) {
        e2eShowMessage(`네트워크 오류: ${err.message || err}`);
      }
    }

    async function e2eAttachRun() {
      const inputEl = document.getElementById('e2eRunIdInput');
      const runId = inputEl ? inputEl.value.trim() : '';
      if (!runId) {
        e2eShowMessage("연결할 run_id를 입력하세요.");
        return;
      }
      e2eClearMessage();
      try {
        const resp = await fetch(`api/e2e/runs/${runId}`);
        if (resp.status === 200) {
          const data = await resp.json();
          e2eCurrentRunId = runId;
          const idDisplay = document.getElementById('e2eRunIdDisplay');
          if (idDisplay) idDisplay.textContent = runId;
          e2eRenderSnapshot(data);
          const overall = data.overall_status || data.overall_state || 'IDLE';
          if (!e2eIsTerminalStatus(overall)) {
            e2eStartPolling();
          }
        } else {
          const errData = await resp.json().catch(() => ({}));
          const msg = errData.message || `HTTP ${resp.status}`;
          e2eShowMessage(`실행 연결 실패 (${resp.status}): ${msg}`);
        }
      } catch (err) {
        e2eShowMessage(`실행 연결 통신 오류: ${err.message || err}`);
      }
    }

    async function e2eStopRun() {
      if (!e2eCurrentRunId) return;
      const btnStop = document.getElementById('e2eBtnStop');
      if (btnStop) btnStop.disabled = true;
      try {
        const resp = await fetch(`api/e2e/runs/${e2eCurrentRunId}/stop`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        if (resp.status === 200 || resp.status === 202) {
          await e2ePollStatus();
        } else {
          const errData = await resp.json().catch(() => ({}));
          const msg = errData.message || `HTTP ${resp.status}`;
          e2eShowMessage(`세션 중단 실패: ${msg}`);
          if (btnStop) btnStop.disabled = false;
        }
      } catch (err) {
        e2eShowMessage(`세션 중단 통신 오류: ${err.message || err}`);
        if (btnStop) btnStop.disabled = false;
      }
    }

    async function e2eHouseholdAction(householdId, action) {
      if (!e2eCurrentRunId) return;
      try {
        const resp = await fetch(`api/e2e/runs/${e2eCurrentRunId}/households/${householdId}/${action}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        if (resp.status === 200 || resp.status === 202) {
          await e2ePollStatus();
        } else {
          const errData = await resp.json().catch(() => ({}));
          const msg = errData.message || `HTTP ${resp.status}`;
          e2eShowMessage(`가구 ${householdId} ${action} 실패: ${msg}`);
        }
      } catch (err) {
        e2eShowMessage(`가구 ${householdId} ${action} 통신 오류: ${err.message || err}`);
      }
    }

    function e2eStartPolling() {
      if (e2ePollTimer) {
        clearInterval(e2ePollTimer);
        e2ePollTimer = null;
      }
      e2ePollStatus();
      e2ePollTimer = setInterval(e2ePollStatus, e2ePollIntervalMs);
    }

    async function e2ePollStatus() {
      if (!e2eCurrentRunId) {
        if (e2ePollTimer) {
          clearInterval(e2ePollTimer);
          e2ePollTimer = null;
        }
        return;
      }
      try {
        const resp = await fetch(`api/e2e/runs/${e2eCurrentRunId}`);
        if (!resp.ok) {
          e2eShowMessage(`상태 조회 실패 (HTTP ${resp.status})`);
          // 요구사항 7: 4xx, 5xx 오류 시에도 폴링 유지
          return;
        }
        const data = await resp.json();
        e2eRenderSnapshot(data);
        const overall = data.overall_status || data.overall_state || '';
        if (e2eIsTerminalStatus(overall)) {
          if (e2ePollTimer) {
            clearInterval(e2ePollTimer);
            e2ePollTimer = null;
          }
        }
      } catch (err) {
        e2eShowMessage(`상태 폴링 통신 오류: ${err.message || err}`);
        // 요구사항 7: 네트워크 오류 시에도 폴링 유지
      }
    }

    function e2eRenderSnapshot(snapshot) {
      e2eLastSnapshot = snapshot;
      const overallStatus = snapshot.overall_status || snapshot.overall_state || 'IDLE';
      e2eSetOverallStatus(overallStatus);

      // BURST 실행은 실시간 전력 흐름이 아닌 MQTT 발행 진행 상태로 흐름 패널에 표시한다.
      powerflowRenderE2E(snapshot);

      const runIdDisplay = document.getElementById('e2eRunIdDisplay');
      if (runIdDisplay) {
        runIdDisplay.textContent = snapshot.run_id || e2eCurrentRunId || '—';
      }

      const tbody = document.getElementById('e2eHouseholdTableBody');
      const households = snapshot.households || [];

      households.forEach(h => {
        const hId = h.household_id;
        const st = h.state || 'STARTING';
        const plannedSlots = h.planned_virtual_slots || 0;
        const settledSlots = h.settled_virtual_slots || 0;

        // [2] 진행률 분모를 계획 가상 슬롯(planned_virtual_slots)으로 고정
        // 주의: 진행률과 발행 수는 분모가 다릅니다:
        // - 진행률 분모: planned_virtual_slots (정산 슬롯 기준, 결측 구간 포함 전체 슬롯)
        // - 발행 수 분모: planned_publish_samples (실제 MQTT 메시지 발행 예정 수)
        const progressPct = plannedSlots > 0 ? Math.min(100, Math.max(0, Math.round((settledSlots / plannedSlots) * 100))) : 0;
        const publishedStr = Number(h.published_samples || 0).toLocaleString();
        const plannedPublishStr = Number(h.planned_publish_samples || 0).toLocaleString();
        const omittedStr = Number(h.omitted_samples || 0).toLocaleString();

        // [1] 가구별 버튼 활성화: overall_status 가 아니라 households[].state 로 제어
        // - pause 활성화: state == "RUNNING" 일 때만
        // - resume 활성화: state == "PAUSED" 일 때만
        // - stop 활성화: state 가 RUNNING, PAUSING, PAUSED 중 하나일 때만
        // - PAUSING, STOPPING 등 전이 상태에서는 해당 요청 버튼 비활성화 (중복 요청 방지)
        // - STOPPED, COMPLETED, FAILED 는 모두 비활성화
        const pauseDisabled = (st !== 'RUNNING');
        const resumeDisabled = (st !== 'PAUSED');
        const stopDisabled = !(st === 'RUNNING' || st === 'PAUSING' || st === 'PAUSED');

        let tr = document.getElementById(`e2eRow_${hId}`);
        let isNewRow = false;
        if (!tr) {
          tr = document.createElement('tr');
          tr.id = `e2eRow_${hId}`;
          isNewRow = true;
        }

        let badgeClass = 'status-normal';
        if (st === 'RUNNING') badgeClass = 'status-running';
        else if (st === 'PAUSED' || st === 'PAUSING') badgeClass = 'status-missed';
        else if (st === 'STOPPED' || st === 'STOPPING') badgeClass = 'badge-hh-stopped';
        else if (st === 'COMPLETED') badgeClass = 'status-normal';
        else if (st === 'FAILED') badgeClass = 'status-peak';

        let statusBadge = document.getElementById(`e2eStatus_${hId}`);
        if (!statusBadge) {
          statusBadge = document.createElement('span');
          statusBadge.id = `e2eStatus_${hId}`;
        }
        statusBadge.className = `badge-status ${badgeClass}`;
        statusBadge.textContent = st;

        let samplesTd = document.getElementById(`e2eSamples_${hId}`);
        if (!samplesTd) {
          samplesTd = document.createElement('td');
          samplesTd.id = `e2eSamples_${hId}`;
        }
        samplesTd.textContent = `${publishedStr} / ${plannedPublishStr}`;

        let omittedTd = document.getElementById(`e2eOmitted_${hId}`);
        if (!omittedTd) {
          omittedTd = document.createElement('td');
          omittedTd.id = `e2eOmitted_${hId}`;
        }
        omittedTd.textContent = omittedStr;

        let progTd = document.getElementById(`e2eProgress_${hId}`);
        if (!progTd) {
          progTd = document.createElement('td');
          progTd.id = `e2eProgress_${hId}`;
        }
        progTd.textContent = `${progressPct}%`;

        let daysTd = document.getElementById(`e2eDays_${hId}`);
        if (!daysTd) {
          daysTd = document.createElement('td');
          daysTd.id = `e2eDays_${hId}`;
        }
        daysTd.textContent = `${h.completed_days || 0} / ${h.total_days || 1}일`;

        let errEl = document.getElementById(`e2eError_${hId}`);
        if (!errEl) {
          errEl = document.createElement('div');
          errEl.id = `e2eError_${hId}`;
          errEl.style.fontSize = '0.75rem';
          errEl.style.color = '#b91c1c';
        }
        errEl.textContent = h.last_error || '';

        let btnPause = document.getElementById(`e2eBtnPause_${hId}`);
        if (!btnPause) {
          btnPause = document.createElement('button');
          btnPause.id = `e2eBtnPause_${hId}`;
          btnPause.className = 'btn btn-secondary';
          btnPause.style.fontSize = '0.75rem';
          btnPause.style.padding = '3px 8px';
          btnPause.textContent = '일시정지';
          btnPause.onclick = () => e2eHouseholdAction(hId, 'pause');
        }
        btnPause.disabled = pauseDisabled;

        let btnResume = document.getElementById(`e2eBtnResume_${hId}`);
        if (!btnResume) {
          btnResume = document.createElement('button');
          btnResume.id = `e2eBtnResume_${hId}`;
          btnResume.className = 'btn btn-secondary';
          btnResume.style.fontSize = '0.75rem';
          btnResume.style.padding = '3px 8px';
          btnResume.style.marginLeft = '4px';
          btnResume.textContent = '재개';
          btnResume.onclick = () => e2eHouseholdAction(hId, 'resume');
        }
        btnResume.disabled = resumeDisabled;

        let btnStop = document.getElementById(`e2eBtnStop_${hId}`);
        if (!btnStop) {
          btnStop = document.createElement('button');
          btnStop.id = `e2eBtnStop_${hId}`;
          btnStop.className = 'btn btn-secondary';
          btnStop.style.fontSize = '0.75rem';
          btnStop.style.padding = '3px 8px';
          btnStop.style.marginLeft = '4px';
          btnStop.style.color = '#b91c1c';
          btnStop.textContent = '중지';
          btnStop.onclick = () => e2eHouseholdAction(hId, 'stop');
        }
        btnStop.disabled = stopDisabled;

        if (isNewRow || (tr.children && tr.children.length === 0)) {
          if (tr.replaceChildren) {
            tr.replaceChildren();
          } else {
            tr.innerHTML = '';
          }
          const tdHouse = document.createElement('td');
          tdHouse.innerHTML = `<strong>${hId}</strong>`;
          const tdScen = document.createElement('td');
          tdScen.textContent = h.scenario || h.scenario_id || '';
          if (errEl.textContent) tdScen.appendChild(errEl);

          const tdStatus = document.createElement('td');
          tdStatus.appendChild(statusBadge);

          const tdActions = document.createElement('td');
          tdActions.appendChild(btnPause);
          tdActions.appendChild(btnResume);
          tdActions.appendChild(btnStop);

          tr.appendChild(tdHouse);
          tr.appendChild(tdScen);
          tr.appendChild(tdStatus);
          tr.appendChild(samplesTd);
          tr.appendChild(omittedTd);
          tr.appendChild(progTd);
          tr.appendChild(daysTd);
          tr.appendChild(tdActions);
        }

        if (tbody) {
          const hasPlaceholder = Array.from(tbody.children).some(c => c.textContent && c.textContent.includes('실행 중인 E2E 시나리오가 없습니다'));
          if (hasPlaceholder) {
            if (tbody.replaceChildren) tbody.replaceChildren();
            else tbody.innerHTML = '';
          }
          if (tr.parentNode !== tbody) {
            tbody.appendChild(tr);
          }
        }
      });
    }

    // 초기 E2E 컨트롤 세팅
    e2eOnModeChange();
    e2eLoadScenarios();
