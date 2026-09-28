    // 가전 상태 표시 및 수동 제어
    function clearAllPendingRequests() {
      pendingDeviceRequests.forEach((info) => {
        if (info && info.timeoutId) {
          clearTimeout(info.timeoutId);
        }
      });
      pendingDeviceRequests.clear();
    }

    function clearPendingRequest(device) {
      const info = pendingDeviceRequests.get(device);
      if (info) {
        if (info.timeoutId) {
          clearTimeout(info.timeoutId);
        }
        pendingDeviceRequests.delete(device);
      }
    }

    function isDeviceControllable(device) {
      const curMetrics = latestMetricsByHouse.get(observedHouse);
      const isObservedManual = curMetrics ? (curMetrics.scenario === "manual" || curMetrics.mode === "manual") : false;
      const isObservedRunning = curMetrics ? (curMetrics.status === "running") : false;

      return isServerConnected &&
        isObservedManual &&
        isObservedRunning &&
        !isPaused &&
        !pendingDeviceRequests.has(device);
    }

    function updateApplianceButtonsState() {
      Object.keys(DEVICE_PROFILES).forEach(dev => {
        const chip = document.getElementById(`chip_${dev}`);
        if (chip) {
          const controllable = isDeviceControllable(dev);
          chip.disabled = !controllable;
          chip.style.cursor = controllable ? "pointer" : "not-allowed";
        }
      });
    }

    function renderDeviceState(dev, mode = currentMode, totalP = 0) {
      const chip = document.getElementById(`chip_${dev}`);
      const stLabel = document.getElementById(`state_${dev}`);
      if (!chip || !stLabel) return;

      const devInfo = (latestDeviceStates && latestDeviceStates[dev]) || {};
      const stState = devInfo.state || "OFF";
      const manualHold = !!devInfo.manualHold;
      const isPending = pendingDeviceRequests.has(dev);

      let chipClass = "app-chip";
      let stateText = "OFF";
      let ariaPressed = "false";

      if (stState === "STARTING") {
        chipClass = "app-chip starting";
        stateText = isPending ? "처리 중..." : "기동 중";
        ariaPressed = "true";
      } else if (stState === "RUNNING") {
        if (mode === 'normal_routine' && dev === 'microwave') {
          chipClass = "app-chip active";
          stateText = isPending ? "처리 중..." : "ON";
        } else if (manualHold) {
          chipClass = "app-chip active manual-hold";
          stateText = isPending ? "처리 중..." : "ON · 수동 유지";
        } else {
          const isPeakApp = (dev === "kettle" || dev === "induction") && totalP >= 3000.0;
          chipClass = isPeakApp ? "app-chip active peak-app" : "app-chip active";
          stateText = isPending ? "처리 중..." : "ON";
        }
        ariaPressed = "true";
      } else {
        // OFF
        chipClass = "app-chip";
        stateText = isPending ? "처리 중..." : "OFF";
        ariaPressed = "false";
      }

      if (mode === 'routine_missed' && dev === 'microwave' && stState === "OFF") {
        chipClass = "app-chip missed-target";
        stateText = "미가동 (이상 감시)";
      }

      chip.className = chipClass;
      chip.setAttribute("aria-pressed", ariaPressed);
      stLabel.textContent = stateText;
    }

    function toggleDevice(device) {
      if (!isDeviceControllable(device)) return Promise.resolve();
      const curState = (latestDeviceStates && latestDeviceStates[device]) ? latestDeviceStates[device] : {};
      const currentEnabled = !!curState.enabled;
      const targetEnabled = !currentEnabled;
      return setDeviceEnabled(device, targetEnabled);
    }

    async function setDeviceEnabled(device, enabled) {
      if (pendingDeviceRequests.has(device)) return;

      // 5초 타임아웃 타이머 등록
      const timeoutId = setTimeout(() => {
        if (pendingDeviceRequests.has(device)) {
          clearPendingRequest(device);
          updateApplianceButtonsState();
          renderDeviceState(device);
          const devKo = (DEVICE_PROFILES[device] && DEVICE_PROFILES[device].name_ko) || device;
          showNoticeError(`[타임아웃] '${devKo}' 상태 변경 SSE 응답이 5초 내에 수신되지 않아 취소되었습니다.`);
        }
      }, PENDING_TIMEOUT_MS);

      // pending으로 등록하고 버튼 즉시 비활성화 및 처리 중 표시
      pendingDeviceRequests.set(device, {
        enabled: enabled,
        timeoutId: timeoutId
      });
      updateApplianceButtonsState();

      const stLabel = document.getElementById(`state_${device}`);
      if (stLabel) stLabel.textContent = "처리 중...";

      try {
        const res = await fetch('api/device', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            house: observedHouse,
            device: device,
            enabled: enabled
          })
        });

        if (!res.ok) {
          let errMsg = `가전 제어 실패 (${res.status})`;
          try {
            const errData = await res.json();
            if (errData && errData.message) errMsg = errData.message;
          } catch (_) { }
          clearPendingRequest(device);
          updateApplianceButtonsState();
          renderDeviceState(device);
          showNoticeError(`[가전 제어 오류] ${errMsg}`);
        }
      } catch (err) {
        clearPendingRequest(device);
        updateApplianceButtonsState();
        renderDeviceState(device);
        showNoticeError(`[가전 제어 네트워크 오류] ${err.message || err}`);
      }
    }



    // XSS 방지: textContent 및 안전한 DOM 조립 사용
    function showNoticeError(msg) {
      const notice = document.getElementById('timelineNotice');
      if (notice) {
        notice.style.color = "#b91c1c";
        notice.style.borderColor = "#b91c1c";
        notice.replaceChildren();
        const strong = document.createElement('strong');
        strong.textContent = "[오류 안내] ";
        const span = document.createElement('span');
        span.textContent = String(msg);
        notice.appendChild(strong);
        notice.appendChild(span);
      }
    }

