    // ==========================================
    // 4. UI 갱신 및 시뮬레이션 루프
    // ==========================================
    function tick() {
      currentSec++;
      const metrics = stepPhysicsTick(currentSec);

      // 데이터 버퍼 저장
      historyData.times.push(metrics.sec);
      historyData.activeP.push(metrics.totalP.toFixed(1));
      historyData.reactiveP.push(metrics.totalQ.toFixed(1));
      historyData.apparentS.push(metrics.apparentS.toFixed(1));
      historyData.currents.push(metrics.currentA.toFixed(2));
      historyData.pfs.push(metrics.pf.toFixed(3));
      historyData.voltages.push(metrics.voltage.toFixed(1));
      historyData.activeLabels.push(metrics.activeNames.join(', ') || '대기');

      // 최고 전력 갱신
      if (metrics.totalP > maxPowerRecorded) {
        maxPowerRecorded = metrics.totalP;
      }

      // 통계 카드 갱신
      document.getElementById('valPower').textContent = metrics.totalP.toFixed(1);
      document.getElementById('valApparent').textContent = metrics.apparentS.toFixed(1);
      document.getElementById('valCurrent').textContent = metrics.currentA.toFixed(2);
      document.getElementById('valVoltage').textContent = metrics.voltage.toFixed(1);
      document.getElementById('valPf').textContent = metrics.pf.toFixed(3);
      document.getElementById('valReactive').textContent = metrics.totalQ.toFixed(1);
      document.getElementById('valTime').textContent = `T+${String(currentSec).padStart(2, '0')}s`;
      document.getElementById('valMaxPower').textContent = `${maxPowerRecorded.toFixed(1)} W`;

      // 상태 배지 및 카드 스타일
      const cardPower = document.getElementById('cardPower');
      const badgeStatus = document.getElementById('badgeStatus');
      const timelineNotice = document.getElementById('timelineNotice');

      if (metrics.totalP >= 3000.0) {
        cardPower.className = "stat-card alert-card";
        badgeStatus.className = "badge-status status-peak";
        badgeStatus.textContent = "피크 경보 (3,000W+ 초과!)";
        timelineNotice.style.color = "#b91c1c";
        timelineNotice.style.borderColor = "#b91c1c";
        timelineNotice.innerHTML = `<strong>[피크 경보]</strong> T+${currentSec}s: 소비전력 <strong>${metrics.totalP.toFixed(1)} W</strong> - 전기포트와 인덕션이 동시 기동 중입니다!`;
      } else if (metrics.totalP >= 1000.0) {
        cardPower.className = "stat-card";
        badgeStatus.className = "badge-status status-running";
        badgeStatus.textContent = "가전 가동 중";
        timelineNotice.style.color = "#b8490d";
        timelineNotice.style.borderColor = "rgba(243, 121, 41, 0.3)";
        timelineNotice.innerHTML = `<strong>[진행 중]</strong> T+${currentSec}s: 소비전력 ${metrics.totalP.toFixed(1)} W (가동 가전: ${metrics.activeNames.join(', ')})`;
      } else {
        cardPower.className = "stat-card";
        badgeStatus.className = "badge-status status-normal";
        badgeStatus.textContent = "정상 대기";
        if (currentMode === 'peak' && currentSec < 10) {
          timelineNotice.innerHTML = `<strong>[대기 중]</strong> T+${currentSec}s: 정상 대기전력 상태입니다. <strong>10초에 3,000W+ 피크 발생 예정!</strong> (카운트다운: ${10 - currentSec}초)`;
        } else if (currentMode === 'peak' && currentSec >= 45) {
          timelineNotice.innerHTML = `<strong>[시연 완료]</strong> T+${currentSec}s: 모든 가전이 종료되어 평상시 대기 상태로 복귀했습니다.`;
        } else if (currentMode === 'routine_missed') {
          badgeStatus.className = "badge-status status-missed";
          const simDateVal = (document.getElementById('simDateInput') && document.getElementById('simDateInput').value) || getKstTodayDateString();
          const totalSecKst = 8 * 3600 + 10 * 60 + currentSec;
          const curH = Math.floor(totalSecKst / 3600) % 24;
          const curM = Math.floor((totalSecKst % 3600) / 60);
          const curS = totalSecKst % 60;
          const timeStr = `${simDateVal} ${String(curH).padStart(2, '0')}:${String(curM).padStart(2, '0')}:${String(curS).padStart(2, '0')}`;

          if (currentSec < 299) {
            badgeStatus.textContent = `루틴 누락 감시 (${currentSec}/299s)`;
            timelineNotice.style.color = "#92400e";
            timelineNotice.style.borderColor = "rgba(245, 158, 11, 0.4)";
            timelineNotice.innerHTML = `<strong>[루틴 누락 시연]</strong> KST <strong>${timeStr}</strong>: 08:10 경과 후 전자레인지 미사용 (대기전력 <strong>${metrics.totalP.toFixed(1)} W</strong> 유지 중 / AI 버퍼: ${currentSec}/299)`;
          } else if (currentSec === 299) {
            badgeStatus.textContent = "299개 데이터 충족 (판정 대기)";
            timelineNotice.style.color = "#92400e";
            timelineNotice.style.borderColor = "rgba(245, 158, 11, 0.4)";
            timelineNotice.innerHTML = `<strong>[분석 충족]</strong> KST <strong>${timeStr}</strong>: 299개 분석 입력 데이터 충족 — AI 이상 감지 판정 대기`;
          } else {
            badgeStatus.textContent = "루틴 누락 감시 지속";
          }
        } else if (currentMode === 'normal_routine') {
          const simDateVal = (document.getElementById('simDateInput') && document.getElementById('simDateInput').value) || getKstTodayDateString();
          const totalSecKst = 8 * 3600 + 4 * 60 + 58 + (currentSec - 1);
          const curH = Math.floor(totalSecKst / 3600) % 24;
          const curM = Math.floor((totalSecKst % 3600) / 60);
          const curS = totalSecKst % 60;
          const timeStr = `${simDateVal} ${String(curH).padStart(2, '0')}:${String(curM).padStart(2, '0')}:${String(curS).padStart(2, '0')}`;

          if (currentSec >= 243 && currentSec <= 302) {
            badgeStatus.className = "badge-status status-running";
            badgeStatus.textContent = `정상 루틴 (${currentSec - 242}/60s)`;
            timelineNotice.style.color = "#b8490d";
            timelineNotice.style.borderColor = "rgba(243, 121, 41, 0.4)";
            timelineNotice.innerHTML = `<strong>[정상 루틴 가동]</strong> KST <strong>${timeStr}</strong>: 전자레인지 가동 중 (소비전력 <strong>${metrics.totalP.toFixed(1)} W</strong> / ${currentSec - 242}/60s)`;
          } else {
            badgeStatus.className = "badge-status status-normal";
            badgeStatus.textContent = "정상 일상 진행 중";
            timelineNotice.style.color = "#047857";
            timelineNotice.style.borderColor = "rgba(74, 222, 128, 0.3)";
            timelineNotice.innerHTML = `<strong>[정상 일상]</strong> KST <strong>${timeStr}</strong>: 대기전력 유지 중 (소비전력 <strong>${metrics.totalP.toFixed(1)} W</strong>)`;
          }
        }
      }

      // 가전 칩 상태 갱신
      Object.keys(DEVICE_PROFILES).forEach(dev => {
        const chip = document.getElementById(`chip_${dev}`);
        const stLabel = document.getElementById(`state_${dev}`);
        const isRunning = deviceStates[dev].state === "RUNNING";

        if (isRunning) {
          const isPeakApp = (dev === "kettle" || dev === "induction") && metrics.totalP >= 3000.0;
          chip.className = isPeakApp ? "app-chip active peak-app" : "app-chip active";
          stLabel.textContent = `ON (${deviceStates[dev].p.toFixed(0)}W)`;
        } else if (currentMode === 'routine_missed' && dev === 'microwave') {
          chip.className = "app-chip missed-target";
          stLabel.textContent = "미가동 (이상 감시)";
        } else {
          chip.className = "app-chip";
          stLabel.textContent = "대기";
        }
      });

      // Chart.js 실시간 업데이트
      const label = `${metrics.sec}s`;
      chartMain.data.labels.push(label);
      chartMain.data.datasets[0].data.push(metrics.totalP);
      chartMain.data.datasets[1].data.push(metrics.apparentS);
      chartMain.data.datasets[2].data.push(metrics.totalQ);
      chartMain.update('none');

      chartSecondary.data.labels.push(label);
      chartSecondary.data.datasets[0].data.push(metrics.currentA);
      chartSecondary.data.datasets[1].data.push(metrics.pf);
      chartSecondary.update('none');

      // 종료 조건 (피크: 60s, 루틴 누락: 300s, 정상 일상: 308s)
      if (currentMode === 'peak' && currentSec >= 60) {
        clearInterval(timerId);
        timerId = null;
        isSimulationRunning = false;
        document.getElementById('btnPause').disabled = true;
        badgeStatus.textContent = "시연 완료 (60s 완주)";
        setSimulationDateInputEnabled(true);
      } else if (currentMode === 'routine_missed' && currentSec >= 300) {
        clearInterval(timerId);
        timerId = null;
        isSimulationRunning = false;
        document.getElementById('btnPause').disabled = true;
        badgeStatus.textContent = "H001 루틴 누락 전력 패턴 발행 완료";
        setSimulationDateInputEnabled(true);
      } else if (currentMode === 'normal_routine' && currentSec >= 308) {
        clearInterval(timerId);
        timerId = null;
        isSimulationRunning = false;
        document.getElementById('btnPause').disabled = true;
        badgeStatus.textContent = "H001 정상 일상 전력 패턴 발행 완료";
        setSimulationDateInputEnabled(true);
      }
    }
