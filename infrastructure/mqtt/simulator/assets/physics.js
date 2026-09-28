    // ==========================================
    // 3. 물리 계산 로직 (1초 틱)
    // ==========================================
    function gaussianRandom(mean = 0, stdev = 1) {
      let u = 1 - Math.random();
      let v = Math.random();
      let z = Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
      return z * stdev + mean;
    }

    function stepPhysicsTick(sec) {
      // 1. 전압 AR-1 및 기저부하
      houseEnv.voltage = 0.98 * houseEnv.voltage + 0.02 * 220.0 + gaussianRandom(0, 0.12);
      houseEnv.voltage = Math.min(228, Math.max(212, houseEnv.voltage));
      let baseP = 55.0 + gaussianRandom(0, 0.8);
      let baseQ = baseP * 0.42;

      // 2. 피크 시나리오 이벤트 주입 (sec 기준)
      let activeNames = [];
      let eventNoticeText = null;

      if (currentMode === 'peak') {
        if (sec === 10) {
          // 10초 피크 기동! (전기포트 1700W + 인덕션 1600W)
          deviceStates.kettle = { state: "RUNNING", p: 1720.0, pf: 0.99, session_remaining: 21 };
          deviceStates.induction = { state: "RUNNING", p: 1640.0, pf: 0.93, session_remaining: 35 };
          eventNoticeText = "피크 발생 (전기포트 1,720W + 인덕션 1,640W 동시 기동 -> 3,400W+ 돌파)";
          addEventRow(sec, "피크 경보", "전기포트, 인덕션", "3,400W+ 돌파", eventNoticeText);
        } else if (sec === 31) {
          // 31초 전기포트 종료
          deviceStates.kettle.state = "OFF";
          deviceStates.kettle.p = 0;
          eventNoticeText = "피크 해소 (전기포트 정지 -> 인덕션 단독 가동 지속)";
          addEventRow(sec, "피크 해소", "전기포트 종료", "1,650W 수준", eventNoticeText);
        } else if (sec === 45) {
          // 45초 인덕션 종료
          deviceStates.induction.state = "OFF";
          deviceStates.induction.p = 0;
          eventNoticeText = "정상 복귀 (인덕션 정지 -> 평상시 대기전력 약 60W 복귀)";
          addEventRow(sec, "정상 복귀", "모든 가전 종료", "60W 수준", eventNoticeText);
        }
      } else if (currentMode === 'routine_missed') {
        // 모든 가전 OFF 유지 (기저부하 + 냉장고만 가동)
        Object.keys(deviceStates).forEach(dev => {
          deviceStates[dev].state = "OFF";
          deviceStates[dev].p = 0;
        });

        const totalSecKst = 8 * 3600 + 10 * 60 + sec;
        const curH = Math.floor(totalSecKst / 3600) % 24;
        const curM = Math.floor((totalSecKst % 3600) / 60);
        const curS = totalSecKst % 60;
        const timeStr = `${String(curH).padStart(2, '0')}:${String(curM).padStart(2, '0')}:${String(curS).padStart(2, '0')}`;

        if (sec === 1) {
          eventNoticeText = "08:10 아침 루틴 검증 시작 (전자레인지 미가동 / 대기전력 유지)";
          addEventRow(sec, "루틴 감시", "전자레인지 미가동", `${(baseP + houseEnv.fridge_p).toFixed(0)} W`, eventNoticeText);
        } else if (sec === 100) {
          eventNoticeText = `대기전력 지속 중 (누적 100초, KST ${timeStr})`;
          addEventRow(sec, "루틴 감시", "전자레인지 미가동", `${(baseP + houseEnv.fridge_p).toFixed(0)} W`, eventNoticeText);
        } else if (sec === 200) {
          eventNoticeText = `대기전력 지속 중 (누적 200초, KST ${timeStr})`;
          addEventRow(sec, "루틴 감시", "전자레인지 미가동", `${(baseP + houseEnv.fridge_p).toFixed(0)} W`, eventNoticeText);
        } else if (sec === 299) {
          eventNoticeText = "299개 분석 입력 데이터 충족 — AI 이상 감지 판정 대기";
          addEventRow(sec, "이상 감시", "299개 충족", `${(baseP + houseEnv.fridge_p).toFixed(0)} W`, eventNoticeText);
        } else if (sec === 300) {
          eventNoticeText = "H001 루틴 누락 전력 패턴 발행 완료";
          addEventRow(sec, "시연 완료", "전체 완주", `${(baseP + houseEnv.fridge_p).toFixed(0)} W`, eventNoticeText);
        }
      } else if (currentMode === 'normal_routine') {
        if (sec === 243) {
          deviceStates.microwave = { state: "RUNNING", p: 940.0, pf: 0.91, session_remaining: 60 };
          eventNoticeText = "08:09 아침 정상 루틴 시작 — 전자레인지 가동";
          addEventRow(sec, "정상 루틴", "전자레인지", "940W 가동", eventNoticeText);
        } else if (sec === 303) {
          deviceStates.microwave.state = "OFF";
          deviceStates.microwave.p = 0;
          eventNoticeText = "08:10 이전 전자레인지 60초 사용 완료 — 대기전력 복귀";
          addEventRow(sec, "정상 복귀", "전자레인지 종료", "대기전력 복귀", eventNoticeText);
        } else if (sec === 308) {
          eventNoticeText = "H001 정상 일상 전력 패턴 발행 완료";
          addEventRow(sec, "시연 완료", "전체 완주", "대기전력 유지", eventNoticeText);
        }
      } else if (currentMode === 'random') {
        // 랜덤 가전 트리거
        Object.keys(DEVICE_PROFILES).forEach(dev => {
          let st = deviceStates[dev];
          let prof = DEVICE_PROFILES[dev];
          if (st.state === "OFF" && Math.random() < 0.02) {
            st.state = "RUNNING";
            st.session_remaining = Math.floor(Math.random() * 15) + 10;
            st.p = prof.median_w; // EDA 실측 중앙값 고정 (Python 엔진과 동일)
            st.pf = prof.pf_nominal[0];
            addEventRow(sec, "가전 켜짐", prof.name_ko, `${st.p.toFixed(0)} W`, "랜덤 동작");
          } else if (st.state === "RUNNING") {
            st.session_remaining--;
            if (st.session_remaining <= 0) {
              st.state = "OFF";
              st.p = 0;
              addEventRow(sec, "가전 꺼짐", prof.name_ko, "0 W", "가동 종료");
            }
          }
        });
      }

      // 3. 총합 부하 계산
      let totalP = baseP;
      let totalQ = baseQ;

      Object.keys(deviceStates).forEach(dev => {
        let st = deviceStates[dev];
        if (st.state === "RUNNING") {
          totalP += st.p + gaussianRandom(0, 1.2);
          let q = st.p * Math.sqrt(Math.max(0, 1 - st.pf * st.pf)) / st.pf;
          totalQ += q;
          activeNames.push(DEVICE_PROFILES[dev].name_ko);
        }
      });

      totalP = Math.max(0, totalP);
      totalQ = Math.max(0, totalQ);
      let apparentS = Math.sqrt(totalP * totalP + totalQ * totalQ);
      let pf = apparentS > 1 ? Math.min(1.0, Math.max(0.1, totalP / apparentS)) : 1.0;
      let currentA = apparentS / houseEnv.voltage;

      return {
        sec, totalP, totalQ, apparentS, pf,
        voltage: houseEnv.voltage, currentA,
        activeNames, eventNoticeText
      };
    }
