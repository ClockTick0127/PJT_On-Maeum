    // ==========================================
    // 4-A. 웹 서버(web_server.py) 실제 MQTT 연동 로직
    // ==========================================
    let isServerConnected = false;
    let evtSource = null;

    function connectServerStream() {
      if (evtSource) {
        evtSource.close();
        evtSource = null;
      }
      evtSource = new EventSource('api/stream');
      evtSource.onmessage = (e) => {
        try {
          if (e.data && e.data.startsWith('{')) {
            const m = JSON.parse(e.data);
            processServerMetrics(m);
          }
        } catch (err) {
          console.warn("SSE JSON 파싱 오류:", err);
        }
      };
      evtSource.onerror = (e) => {
        if (currentMode === 'manual') {
          hasReceivedManualSse = false;
          updateApplianceButtonsState();
          document.getElementById('badgeStatus').textContent = "수동 제어 재연결 중...";
          const notice = document.getElementById('timelineNotice');
          if (notice) {
            notice.style.color = "#92400e";
            notice.style.borderColor = "rgba(245, 158, 11, 0.4)";
            notice.replaceChildren();
            const strong = document.createElement('strong');
            strong.textContent = "[연결 재시도] ";
            const span = document.createElement('span');
            span.textContent = "서버 스트림(SSE) 연결이 끊어져 자동 재연결 중입니다. 가전 제어 버튼이 일시 비활성화됩니다.";
            notice.appendChild(strong);
            notice.appendChild(span);
          }
        }
      };
    }

    async function checkServerConnection() {
      const btnManual = document.getElementById('btnManualDemo');
      try {
        const res = await fetch('api/status', { method: 'GET' });
        if (res.ok) {
          const data = await res.json();
          isServerConnected = true;
          if (btnManual) btnManual.disabled = false;
          const indicator = document.getElementById('serverIndicator');
          if (indicator) {
            indicator.className = "badge-status status-normal";
            indicator.innerHTML = `MQTT 브로커 연동 모드 (${data.broker || 'MQTT'}) - 실제 발행 활성화`;
          }

          // 서버 상태 동기화
          isSimulationRunning = Boolean(data.is_running);
          isPaused = Boolean(data.is_paused);
          if (typeof setCurrentSeed === 'function' && typeof data.seed === 'number') {
            setCurrentSeed(data.seed);
          }

          if (typeof data.interval === 'number' && data.interval > 0) {
            const serverMs = Math.round(data.interval * 1000);
            intervalMs = serverMs;
            lastSuccessfulSpeedMs = serverMs;
            const speedSelectEl = document.getElementById('speedSelect');
            if (speedSelectEl) {
              const opt = speedSelectEl.querySelector ? speedSelectEl.querySelector(`option[value="${serverMs}"]`) : null;
              if (opt || (speedSelectEl.options && Array.from(speedSelectEl.options).some(o => o.value === String(serverMs)))) {
                speedSelectEl.value = String(serverMs);
              }
            }
          }

          if (data.active_households && typeof data.active_households === 'object') {
            const restored = Object.entries(data.active_households).map(([house, info]) => ({
              house: house,
              scenario: (info && info.scenario) ? info.scenario : 'random'
            }));
            if (restored.length > 0) {
              activeConfiguredHouseholds = restored;
              if (restored.length === 1) {
                currentMode = restored[0].scenario;
              } else {
                currentMode = (data.current_mode) ? data.current_mode : 'multi';
              }

              // 관찰 가구 드롭다운 복원 (H001 ~ H010 전체 유지)
              rebuildObservedHouseSelect(restored);
            }
          }

          // last_metrics_by_house 복원: 다중 가구 중 일부 완료된 가구 및 최신 메트릭 복원
          latestMetricsByHouse.clear();
          if (data.last_metrics_by_house && typeof data.last_metrics_by_house === 'object') {
            Object.entries(data.last_metrics_by_house).forEach(([house, metrics]) => {
              if (metrics && typeof metrics === 'object') {
                latestMetricsByHouse.set(house, metrics);
                updateHouseholdTableRow(house, metrics.status, metrics.sec, metrics.totalP);
                if (typeof metrics.totalP === 'number') {
                  const curMax = maxPowerByHouse.get(house) || 0.0;
                  if (metrics.totalP > curMax) {
                    maxPowerByHouse.set(house, metrics.totalP);
                  }
                }
              }
            });
          }

          // 현재 관찰 가구의 최신 메트릭이 존재하면 화면 상태 복원
          const obsMetrics = latestMetricsByHouse.get(observedHouse);
          if (obsMetrics) {
            renderObservedHouseMetrics(obsMetrics);
          }

          if (isSimulationRunning) {
            powerflowSetObservedHouse(observedHouse);
            powerflowSetRunState(isPaused ? 'paused' : 'running');
            connectServerStream();
            setTableAndControlsEnabled(false);
            setSimulationDateInputEnabled(false);
            const btnPause = document.getElementById('btnPause');
            if (btnPause) {
              btnPause.disabled = false;
              btnPause.textContent = isPaused ? "재생" : "일시정지";
            }
            const badge = document.getElementById('badgeStatus');
            if (badge) {
              badge.className = isPaused ? "badge-status status-normal" : "badge-status status-running";
              badge.textContent = isPaused ? "일시정지됨" : `시뮬레이션 실행 중 (${activeConfiguredHouseholds.length}개 가구)`;
            }
          } else {
            checkAllHouseholdsFinished();
          }
        } else {
          throw new Error("서버 상태 응답 실패");
        }
      } catch (e) {
        isServerConnected = false;
        if (btnManual) btnManual.disabled = true;
        const indicator = document.getElementById('serverIndicator');
        if (indicator) {
          indicator.className = "badge-status status-running";
          indicator.innerHTML = "브라우저 단독 시각화 모드 (로컬 엔진)";
        }
      }
      updateApplianceButtonsState();
    }

    // 특정 가구의 metrics로 대시보드 통계 카드 및 상태 배너 갱신
    function renderObservedHouseMetrics(m) {
      if (!m) return;
      currentSec = m.sec;

      const house = m.house || observedHouse;
      const isFault = !!(m.sensorFault || m.measurementAvailable === false);
      const isNullPower = m.totalP === null || m.totalP === undefined;

      if (typeof m.totalP === 'number') {
        const curMax = maxPowerByHouse.get(house) || 0.0;
        if (m.totalP > curMax) {
          maxPowerByHouse.set(house, m.totalP);
        }
      }
      const displayMax = maxPowerByHouse.get(house) || 0.0;

      document.getElementById('valPower').textContent = isNullPower ? "—" : m.totalP.toFixed(1);
      document.getElementById('valApparent').textContent = (m.apparentS === null || m.apparentS === undefined) ? "—" : m.apparentS.toFixed(1);
      document.getElementById('valCurrent').textContent = (m.currentA === null || m.currentA === undefined) ? "—" : m.currentA.toFixed(2);
      document.getElementById('valVoltage').textContent = (m.voltage === null || m.voltage === undefined) ? "—" : m.voltage.toFixed(1);
      document.getElementById('valPf').textContent = (m.pf === null || m.pf === undefined) ? "—" : m.pf.toFixed(3);
      document.getElementById('valReactive').textContent = (m.totalQ === null || m.totalQ === undefined) ? "—" : m.totalQ.toFixed(1);
      document.getElementById('valTime').textContent = `T+${String(m.sec).padStart(2, '0')}s`;
      document.getElementById('valMaxPower').textContent = `${displayMax.toFixed(1)} W`;

      const cardPower = document.getElementById('cardPower');
      const badgeStatus = document.getElementById('badgeStatus');
      const timelineNotice = document.getElementById('timelineNotice');

      const scenario = m.scenario || m.mode;
      const status = m.status || 'running';
      const timeLabel = getSimulationDateTimeLabel(m);

      if (status === 'completed') {
        badgeStatus.className = "badge-status status-normal";
        if (scenario === 'sensor_fault') {
          const faultDur = m.faultDurationSec ?? 120;
          badgeStatus.textContent = `${m.house} 센서 결측 시연 완료`;
          timelineNotice.style.color = "#047857";
          timelineNotice.style.borderColor = "rgba(74, 222, 128, 0.4)";
          timelineNotice.innerHTML = `<strong>[시연 완료]</strong> ${timeLabel}${m.house} ${faultDur}초 결측 구간 종료 및 정상 복구 완료`;
        } else if (scenario === 'normal_routine') {
          badgeStatus.textContent = "H001 정상 일상 전력 패턴 발행 완료";
          timelineNotice.style.color = "#047857";
          timelineNotice.style.borderColor = "rgba(74, 222, 128, 0.4)";
          timelineNotice.innerHTML = `<strong>[시연 완료]</strong> ${timeLabel}H001 정상 일상 전력 패턴 발행 완료`;
        } else if (scenario === 'routine_missed') {
          badgeStatus.textContent = "H001 루틴 누락 전력 패턴 발행 완료";
          timelineNotice.style.color = "#92400e";
          timelineNotice.style.borderColor = "rgba(245, 158, 11, 0.4)";
          timelineNotice.innerHTML = `<strong>[시연 완료]</strong> ${timeLabel}H001 루틴 누락 전력 패턴 발행 완료`;
        } else {
          badgeStatus.textContent = `${m.house} 시연 완료 (${m.sec}s)`;
          timelineNotice.style.color = "#047857";
          timelineNotice.style.borderColor = "rgba(74, 222, 128, 0.4)";
          timelineNotice.innerHTML = `<strong>[가구 ${m.house} 완료]</strong> ${timeLabel}T+${m.sec}s: 시연 목표 달성 후 정상 완료되었습니다.`;
        }
      } else if (isFault) {
        cardPower.className = "stat-card";
        badgeStatus.className = "badge-status status-fault";
        const faultDur = m.faultDurationSec ?? 120;
        const elapsed = m.gapElapsedSec ?? (m.sec - 10);
        const remaining = m.gapRemainingSec ?? Math.max(0, faultDur - elapsed);
        badgeStatus.textContent = `${m.house} 센서 고장 (${elapsed}/${faultDur}초)`;
        timelineNotice.style.color = "#92400e";
        timelineNotice.style.borderColor = "rgba(245, 158, 11, 0.4)";
        timelineNotice.innerHTML = `<strong>[센서 고장 중]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 센서 결측 (${faultDur}초 결측 중 <strong>${elapsed}초 경과</strong>, 잔여 ${remaining}초, MQTT 발행 중단)`;
      } else if (typeof m.totalP === 'number' && m.totalP >= 3000.0) {
        cardPower.className = "stat-card alert-card";
        badgeStatus.className = "badge-status status-peak";
        badgeStatus.textContent = `${m.house} 피크 경보 (3,000W+ 초과!)`;
        timelineNotice.style.color = "#b91c1c";
        timelineNotice.style.borderColor = "#b91c1c";
        timelineNotice.innerHTML = `<strong>[피크 발생]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 소비전력 <strong>${m.totalP.toFixed(1)} W</strong> - 3,000W+ 초과 피크 발생!`;
      } else if (scenario === 'normal_routine') {
        cardPower.className = "stat-card";
        const isMicrowaveOn = m.devices && m.devices.microwave && m.devices.microwave.enabled;
        if (isMicrowaveOn) {
          badgeStatus.className = "badge-status status-running";
          badgeStatus.textContent = `${m.house} 정상 루틴 (전자레인지 가동 중)`;
          timelineNotice.style.color = "#b8490d";
          timelineNotice.style.borderColor = "rgba(243, 121, 41, 0.4)";
          timelineNotice.innerHTML = `<strong>[아침 루틴 가동]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 전자레인지 가동 중 (총 전력: <strong>${(m.totalP || 0).toFixed(1)} W</strong>, 08:10 이전 정상 루틴 60초)`;
        } else {
          badgeStatus.className = "badge-status status-normal";
          badgeStatus.textContent = `${m.house} 정상 일상 루틴 진행 중`;
          timelineNotice.style.color = "#047857";
          timelineNotice.style.borderColor = "rgba(74, 222, 128, 0.3)";
          timelineNotice.innerHTML = `<strong>[정상 일상 진행 중]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 대기전력 유지 중 (총 전력: <strong>${(m.totalP || 0).toFixed(1)} W</strong>)`;
        }
      } else if (scenario === 'manual') {
        cardPower.className = "stat-card";
        badgeStatus.className = "badge-status status-running";
        badgeStatus.textContent = `${m.house} 수동 제어 모드`;
        timelineNotice.style.color = "#d95e12";
        timelineNotice.style.borderColor = "rgba(167, 139, 250, 0.4)";
        const actDevs = (m.activeNames && m.activeNames.length > 0) ? m.activeNames.join(', ') : "가전 대기(OFF)";
        timelineNotice.innerHTML = `<strong>[가전 수동 제어]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 총 전력 <strong>${(m.totalP || 0).toFixed(1)} W</strong> (가전: ${actDevs})`;
      } else if (scenario === 'routine_missed') {
        cardPower.className = "stat-card";
        badgeStatus.className = "badge-status status-missed";
        badgeStatus.textContent = "H001 루틴 누락 전력 패턴 발행 중";
        timelineNotice.style.color = "#92400e";
        timelineNotice.style.borderColor = "rgba(245, 158, 11, 0.4)";
        timelineNotice.innerHTML = `<strong>[루틴 누락 감시]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 루틴 누락 전력 패턴 발행 중 (총 전력: <strong>${(m.totalP || 0).toFixed(1)} W</strong>, 대기전력 유지)`;
      } else if (scenario === 'sensor_fault') {
        cardPower.className = "stat-card";
        badgeStatus.className = "badge-status status-normal";
        if (m.sec <= 10) {
          badgeStatus.textContent = `${m.house} 정상 계측 (고장 대기)`;
          timelineNotice.style.color = "#b8490d";
          timelineNotice.style.borderColor = "rgba(243, 121, 41, 0.3)";
          timelineNotice.innerHTML = `<strong>[정상 대기]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 정상 계측 중 (소비전력 <strong>${(m.totalP || 0).toFixed(1)} W</strong>, cycle 11 센서 고장 예정)`;
        } else {
          badgeStatus.textContent = `${m.house} 센서 복구 정상 계측`;
          timelineNotice.style.color = "#047857";
          timelineNotice.style.borderColor = "rgba(74, 222, 128, 0.3)";
          timelineNotice.innerHTML = `<strong>[센서 복구]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 센서 정상 복구 완료 (소비전력 <strong>${(m.totalP || 0).toFixed(1)} W</strong> 실시간 발행 중)`;
        }
      } else {
        cardPower.className = "stat-card";
        badgeStatus.className = "badge-status status-normal";
        badgeStatus.textContent = `${m.house} 실시간 발행 중`;
        timelineNotice.style.color = "#b8490d";
        timelineNotice.style.borderColor = "rgba(243, 121, 41, 0.3)";
        timelineNotice.innerHTML = `<strong>[MQTT 발행 중]</strong> ${timeLabel}T+${m.sec}s: 가구 ${m.house} 전력 ${(m.totalP || 0).toFixed(1)} W`;
      }

      // 이벤트 로그 기록 (현재 관찰 가구인 경우에만 추가)
      if (m.eventNoticeText) {
        let evType = "상태 안내";
        if (m.eventNoticeText.includes("시나리오 완료") || m.eventNoticeText.includes("시연 완료")) evType = "시연 완료";
        else if (m.eventNoticeText.includes("센서 복구")) evType = "센서 복구";
        else if (m.eventNoticeText.includes("고장 시작") || m.eventNoticeText.includes("센서 고장")) evType = "센서 고장";
        else if (m.eventNoticeText.includes("피크 발생")) evType = "피크 경보";
        else if (m.eventNoticeText.includes("피크 해소")) evType = "피크 해소";
        else if (m.eventNoticeText.includes("정상 복귀")) evType = "정상 복귀";
        else if (m.eventNoticeText.includes("이상치") || m.eventNoticeText.includes("버퍼 충족")) evType = "이상치 감지";
        else if (m.eventNoticeText.includes("루틴 검증") || m.eventNoticeText.includes("루틴 누락")) evType = "루틴 감시";
        else if (m.eventNoticeText.includes("아침 루틴") || m.eventNoticeText.includes("정상 일상")) evType = "정상 루틴";

        const powerStr = (typeof m.totalP === 'number') ? `${m.totalP.toFixed(0)} W` : "측정 없음";
        const devStr = isFault ? "센서 블랙아웃" : ((m.activeNames || []).join(', ') || (scenario === 'routine_missed' ? '전자레인지 미가동' : '가전 정지'));
        addEventRow(m.sec, `[${m.house}] ${evType}`, devStr, powerStr, m.eventNoticeText);
      }

      // 가전 상태 칩 갱신
      if (m.devices && typeof m.devices === 'object') {
        latestDeviceStates = m.devices;
        Object.keys(DEVICE_PROFILES).forEach(dev => {
          const devInfo = m.devices[dev] || {};
          const pendingInfo = pendingDeviceRequests.get(dev);

          if (pendingInfo && devInfo.enabled === pendingInfo.enabled) {
            clearPendingRequest(dev);
          }
          renderDeviceState(dev, scenario, m.totalP || 0);
        });
      }
      updateApplianceButtonsState();
    }

    // E2E 결정적 시나리오 관찰 가구 화면 렌더링 (레거시 상태 배지, 타임라인 배너, 가구 목록 테이블 등은 변경하지 않음)
    function renderE2EObservedHouseMetrics(m, isFault) {
      if (!m) return;
      currentSec = m.sec;

      const house = m.house || observedHouse;
      const displayMax = maxPowerByHouse.get(house) || 0.0;
      const isNullPower = isFault || m.totalP === null || m.totalP === undefined;

      // 전력 메트릭 카드 갱신 (결측 시 이전 값을 남기지 않고 "—" 표시)
      const pEl = document.getElementById('valPower');
      if (pEl) pEl.textContent = isNullPower ? "—" : m.totalP.toFixed(1);

      const appEl = document.getElementById('valApparent');
      if (appEl) appEl.textContent = (isFault || m.apparentS === null || m.apparentS === undefined) ? "—" : m.apparentS.toFixed(1);

      const curEl = document.getElementById('valCurrent');
      if (curEl) curEl.textContent = (isFault || m.currentA === null || m.currentA === undefined) ? "—" : m.currentA.toFixed(2);

      const vEl = document.getElementById('valVoltage');
      if (vEl) vEl.textContent = (isFault || m.voltage === null || m.voltage === undefined) ? "—" : m.voltage.toFixed(1);

      const pfEl = document.getElementById('valPf');
      if (pfEl) pfEl.textContent = (isFault || m.pf === null || m.pf === undefined) ? "—" : m.pf.toFixed(3);

      const qEl = document.getElementById('valReactive');
      if (qEl) qEl.textContent = (isFault || m.totalQ === null || m.totalQ === undefined) ? "—" : m.totalQ.toFixed(1);

      const timeEl = document.getElementById('valTime');
      if (timeEl) timeEl.textContent = m.simTimeKst ? m.simTimeKst : `T+${String(m.sec).padStart(2, '0')}s`;

      const maxPEl = document.getElementById('valMaxPower');
      if (maxPEl) maxPEl.textContent = `${displayMax.toFixed(1)} W`;

      // 가전 상태 칩 갱신
      if (m.devices && typeof m.devices === 'object') {
        latestDeviceStates = m.devices;
        Object.keys(DEVICE_PROFILES).forEach(dev => {
          renderDeviceState(dev, m.scenario || 'e2e', isFault ? 0 : (m.totalP || 0));
        });
      }
      updateApplianceButtonsState();
    }

    // 서버에서 날아온 실제 MQTT 계측치 SSE 렌더링
    function processServerMetrics(m) {
      const house = m.house || "H001";
      latestMetricsByHouse.set(house, m);
      const isFault = !!(m.sensorFault || m.measurementAvailable === false);
      const isE2E = (m && m.source === "E2E");

      // 가구별 최고 전력 독립 갱신
      if (typeof m.totalP === 'number') {
        const curMax = maxPowerByHouse.get(house) || 0.0;
        if (m.totalP > curMax) {
          maxPowerByHouse.set(house, m.totalP);
        }
      }

      // 가구별 독립 히스토리 버퍼에 저장
      const hist = getOrCreateHistory(house);
      const timeLabel = m.simTimeKst || new Date().toLocaleTimeString('ko-KR');
      hist.times.push(timeLabel);
      hist.activeP.push(isFault ? null : m.totalP);
      hist.reactiveP.push(isFault ? null : m.totalQ);
      hist.apparentS.push(isFault ? null : m.apparentS);
      hist.currents.push(isFault ? null : m.currentA);
      hist.pfs.push(isFault ? null : m.pf);
      hist.voltages.push(isFault ? null : m.voltage);
      hist.activeLabels.push(isFault ? (isE2E ? '계측 결측' : '센서 고장') : ((m.activeNames || []).join(', ')));

      const maxHistoryLen = Math.max(150, (m.faultDurationSec ?? 120) + 30);
      if (hist.times.length > maxHistoryLen) {
        hist.times.shift();
        hist.activeP.shift();
        hist.reactiveP.shift();
        hist.apparentS.shift();
        hist.currents.shift();
        hist.pfs.shift();
        hist.voltages.shift();
        hist.activeLabels.shift();
      }

      if (!isE2E) {
        // [레거시 전용 경로]
        // 가구 테이블 행 상태(뱃지, 전력) 갱신
        updateHouseholdTableRow(house, m.status, m.sec, m.totalP, isFault, m.gapElapsedSec, m.faultDurationSec ?? 120);

        // 관찰 가구 드롭다운 옵션 유지 (running, completed 모두 유지)
        ensureHouseInObservedSelect(house);
      }

      // 현재 사용자가 보고 있는 관찰 가구인 경우 화면 및 차트 실시간 갱신
      if (house === observedHouse) {
        powerflowApplyRealtime(m);
        if (isE2E) {
          renderE2EObservedHouseMetrics(m, isFault);
        } else {
          renderObservedHouseMetrics(m);
        }

        const label = timeLabel;
        chartMain.data.labels.push(label);
        chartMain.data.datasets[0].data.push(isFault ? null : m.totalP);
        chartMain.data.datasets[1].data.push(isFault ? null : m.apparentS);
        chartMain.data.datasets[2].data.push(isFault ? null : m.totalQ);
        if (chartMain.data.labels.length > maxHistoryLen) {
          chartMain.data.labels.shift();
          chartMain.data.datasets.forEach(ds => ds.data.shift());
        }
        chartMain.update('none');

        chartSecondary.data.labels.push(label);
        chartSecondary.data.datasets[0].data.push(isFault ? null : m.currentA);
        chartSecondary.data.datasets[1].data.push(isFault ? null : m.pf);
        if (chartSecondary.data.labels.length > maxHistoryLen) {
          chartSecondary.data.labels.shift();
          chartSecondary.data.datasets.forEach(ds => ds.data.shift());
        }
        chartSecondary.update('none');
      }

      if (!isE2E) {
        // 모든 실행 가구가 완료되었는지 검사 (레거시 전용)
        checkAllHouseholdsFinished();
      }
    }

    function checkAllHouseholdsFinished() {
      if (!activeConfiguredHouseholds || activeConfiguredHouseholds.length === 0) return;
      const allFinished = activeConfiguredHouseholds.every(item => {
        const data = latestMetricsByHouse.get(item.house);
        return data && (data.status === 'completed' || data.status === 'stopped');
      });

      if (allFinished) {
        if (evtSource) {
          evtSource.close();
          evtSource = null;
        }
        isSimulationRunning = false;
        powerflowSetRunState('completed');
        document.getElementById('btnPause').disabled = true;
        document.getElementById('badgeStatus').className = "badge-status status-normal";
        if (activeConfiguredHouseholds.length === 1) {
          const m = latestMetricsByHouse.get(activeConfiguredHouseholds[0].house);
          const singleScenario = (m && (m.scenario || m.mode)) || activeConfiguredHouseholds[0].scenario;
          const singleHouse = activeConfiguredHouseholds[0].house;
          if (singleScenario === 'normal_routine') {
            document.getElementById('badgeStatus').textContent = "H001 정상 일상 전력 패턴 발행 완료";
          } else if (singleScenario === 'routine_missed') {
            document.getElementById('badgeStatus').textContent = "H001 루틴 누락 전력 패턴 발행 완료";
          } else if (singleScenario === 'sensor_fault') {
            document.getElementById('badgeStatus').textContent = `${singleHouse} 센서 결측 시연 완료`;
          } else {
            document.getElementById('badgeStatus').textContent = `${singleHouse} 시연 완료`;
          }
        } else {
          document.getElementById('badgeStatus').textContent = "전체 가구 시연 완료";
        }
        setTableAndControlsEnabled(true);
      }
    }

    // 관찰 대상 가구 변경 핸들러
    function changeObservedHouse(targetHouse) {
      observedHouse = targetHouse;
      powerflowSetObservedHouse(targetHouse);
      if (!isSimulationRunning && e2eCurrentRunId && e2eLastSnapshot && e2eLastSnapshot.run_id === e2eCurrentRunId) {
        powerflowRenderE2E(e2eLastSnapshot);
      }
      const badgeEl = document.getElementById('observedHouseBadge');
      if (badgeEl) badgeEl.textContent = observedHouse;

      const hist = getOrCreateHistory(observedHouse);
      const m = latestMetricsByHouse.get(observedHouse);
      const houseMax = maxPowerByHouse.get(observedHouse) || 0.0;
      document.getElementById('valMaxPower').textContent = `${houseMax.toFixed(1)} W`;

      // 메인/보조 차트 전체 재렌더링
      chartMain.data.labels = [...hist.times];
      chartMain.data.datasets[0].data = [...hist.activeP];
      chartMain.data.datasets[1].data = [...hist.apparentS];
      chartMain.data.datasets[2].data = [...hist.reactiveP];
      chartMain.update();

      chartSecondary.data.labels = [...hist.times];
      chartSecondary.data.datasets[0].data = [...hist.currents];
      chartSecondary.data.datasets[1].data = [...hist.pfs];
      chartSecondary.update();

      if (m) {
        renderObservedHouseMetrics(m);
      } else {
        document.getElementById('valPower').textContent = "0.0";
        document.getElementById('valApparent').textContent = "0.0";
        document.getElementById('valCurrent').textContent = "0.00";
        document.getElementById('valVoltage').textContent = "220.0";
        document.getElementById('valPf').textContent = "1.000";
        document.getElementById('valReactive').textContent = "0.0";
        document.getElementById('valTime').textContent = "T+00s";
        Object.keys(DEVICE_PROFILES).forEach(dev => {
          renderDeviceState(dev, 'random', 0);
        });
      }
      updateApplianceButtonsState();
    }
