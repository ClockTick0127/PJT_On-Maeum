(function () {
  'use strict';

  // 위 카드: 실행 시각에 맞춘 평소 전자레인지 사용 파형. 발행하지 않는다.
  const TOTAL_SECONDS = 329;
  const USE_START = 30;
  const USE_END = 90;
  const LIMIT_AT = 90;
  function currentKstSeconds() {
    const now = new Date(Date.now() + 9 * 60 * 60 * 1000);
    return now.getUTCHours() * 3600 + now.getUTCMinutes() * 60 + now.getUTCSeconds();
  }
  let startSeconds = currentKstSeconds();

  // 아래 카드: 평소 기록과 같은 시간에 미사용 전력을 발행하는 H001 routine_missed_demo 실행
  const LIVE_HOUSE = 'H001';
  const LIVE_SCENARIO = 'routine_missed_demo';
  const FALLBACK_SCENARIO = 'manual';
  const LIVE_APPLIANCE = 'microwave';
  const LIVE_TOTAL = TOTAL_SECONDS + 1;
  const USUAL_USE_SEC = 60;
  // 웹 Manager의 첫 번째 표본은 sec=1이며, 첫 표본 시각으로 두 그래프의 시작점을 맞춘다.
  const POWER_MAX = 1200;
  const LIVE_VIEW_FROM = 1;
  const LIVE_VIEW_TO = LIVE_TOTAL;
  const DEFAULT_VIEW_SECONDS = 120;
  let viewSeconds = DEFAULT_VIEW_SECONDS;
  let viewFrom = 0;
  let viewFocus = 0;
  let replayRedraw = null;
  let liveRedraw = null;

  const COLORS = { normal: '#d95e12', anomaly: '#b91c1c', grid: 'rgba(214, 211, 209, .55)', tick: '#78716c', band: 'rgba(243, 121, 41, .10)', marker: '#b91c1c' };

  function seededNoise(index, salt) {
    const value = Math.sin((index + 1) * 12.9898 + salt * 78.233) * 43758.5453;
    return (value - Math.floor(value)) * 2 - 1;
  }

  function standbyPower(index, salt) {
    return 56 + Math.sin(index / 9 + salt) * 2.4 + Math.sin(index / 23) * 1.5 + seededNoise(index, salt) * 1.6;
  }

  // 평소에는 시작 30초 뒤부터 전자레인지를 60초간 사용한다.
  function createSeries() {
    const normal = [];
    for (let second = 0; second <= TOTAL_SECONDS; second += 1) {
      let appliance = 0;
      if (second >= USE_START && second < USE_END) {
        const inrush = second === USE_START ? 330 : second === USE_START + 1 ? 180 : 0;
        appliance = 940 + inrush + seededNoise(second, 8) * 4;
      }
      normal.push(Math.round((standbyPower(second, 1) + appliance) * 10) / 10);
    }
    return { normal };
  }

  function formatDuration(seconds) {
    const safe = Math.max(0, Math.round(seconds));
    return `${Math.floor(safe / 60)}:${String(safe % 60).padStart(2, '0')}`;
  }

  function formatClock(absolute) {
    const time = ((absolute % 86400) + 86400) % 86400;
    const hours = Math.floor(time / 3600);
    const minutes = Math.floor((time % 3600) / 60);
    const seconds = time % 60;
    return [hours, minutes, seconds].map(value => String(value).padStart(2, '0')).join(':');
  }

  function formatTime(offset) {
    return formatClock(startSeconds + offset);
  }

  function setStartSeconds(seconds) {
    startSeconds = ((seconds % 86400) + 86400) % 86400;
  }

  function refreshTimeCopy() {
    const start = formatTime(0);
    const use = formatTime(USE_START);
    const until = formatTime(USE_END);
    const end = formatTime(TOTAL_SECONDS);
    const copy = {
      normalDescription: `${start}~${end} 기록에서 평소에는 ${use}에 전자레인지를 켜고 약 1분 뒤 끕니다.`,
      liveDescription: `같은 ${start}~${end} 구간에 H001의 측정값 ${LIVE_TOTAL}개를 발행합니다.`,
      normalUseLegend: `평소 사용 구간 (${use}~${until})`,
      normalDeadlineLegend: `${until} 사용 마감 시각`,
      liveUseLegend: `평소 사용 구간 (${use}~${until})`,
      rangeStartLabel: start,
      rangeEndLabel: end
    };
    for (const [id, value] of Object.entries(copy)) {
      const element = document.getElementById(id);
      if (element) element.textContent = value;
    }
    const normalChart = document.getElementById('normalChart');
    if (normalChart) normalChart.setAttribute('aria-label', `평소 ${use}부터 전자레인지를 약 1분 사용하고 끄는 정상 전력 파형`);
    const anomalyChart = document.getElementById('anomalyChart');
    if (anomalyChart) anomalyChart.setAttribute('aria-label', `평소 ${use}부터 ${until}까지의 사용 구간을 음영으로 표시하고, 금일에는 대기전력만 유지되는 실시간 발행 전력 파형`);
    updateSharedView(viewFocus, true);
  }

  function parseClock(text) {
    const match = /^(\d{2}):(\d{2}):(\d{2})$/.exec(String(text || ''));
    if (!match) return null;
    return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]);
  }

  function storyFor(position) {
    if (position < USE_START) {
      return {
        index: '01 / 03', eyebrow: '대기전력 관찰',
        title: '평소 사용 전에는 대기전력만 흐릅니다.',
        body: '공유기·셋톱박스 등의 대기전력에 냉장고 전력이 더해져 기저부하가 유지됩니다.'
      };
    }
    if (position < USE_END) {
      return {
        index: '02 / 03', eyebrow: '평소 사용',
        title: `평소에는 ${formatTime(USE_START)}에 전자레인지로 약 1분 데웁니다.`,
        body: '전자레인지는 켜는 순간 돌입 전류로 잠깐 튄 뒤 약 940W를 끊김 없이 유지합니다. 끄면 바로 대기전력으로 돌아옵니다.'
      };
    }
    return {
      index: '03 / 03', eyebrow: '오늘과 비교',
      title: '오늘은 사용해야 할 시간에 전자레인지를 켜지 않았습니다.',
      body: `아래 실시간 그래프에는 같은 ${formatTime(0)}~${formatTime(TOTAL_SECONDS)} 구간에 대기전력만 이어집니다. AI 판정은 저장된 루틴 기준 시각을 따릅니다.`
    };
  }

  // ---- 실시간 발행 상태 (DOM과 분리된 순수 함수: 테스트 대상) ----
  function createLiveState(scenario = LIVE_SCENARIO) {
    return { scenario, power: new Array(LIVE_TOTAL + 1).fill(null), lastSec: 0, received: 0, baseSeconds: null, simTime: null, simDate: null, status: 'idle', onSec: null, offSec: null, applianceOn: false };
  }

  function applyLiveSample(state, sample) {
    if (!sample || sample.house !== LIVE_HOUSE || sample.scenario !== state.scenario) return false;
    const sec = Number(sample.sec);
    if (!Number.isInteger(sec) || sec < 0 || sec > LIVE_TOTAL) return false;
    if (state.power[sec] === null) state.received += 1;
    state.power[sec] = sample.measurementAvailable === false ? null : Number(sample.totalP);
    state.lastSec = Math.max(state.lastSec, sec);
    const clock = parseClock(sample.simTimeKst);
    if (clock !== null) {
      if (state.baseSeconds === null) state.baseSeconds = clock - sec;
      state.simTime = sample.simTimeKst;
    }
    if (sample.simDateKst) state.simDate = sample.simDateKst;
    if (sample.status) state.status = sample.status;
    const appliance = sample.devices && sample.devices[LIVE_APPLIANCE];
    if (appliance && sec >= state.lastSec) {
      if (appliance.enabled && state.onSec === null) state.onSec = sec;
      if (!appliance.enabled && state.applianceOn && state.offSec === null) state.offSec = sec;
      state.applianceOn = Boolean(appliance.enabled);
    }
    return true;
  }

  // 켠 시각부터 흐른 초 (분석 서비스와 같은 started_at 기준 경과 시간). 꺼진 뒤에는 꺼진 시각까지.
  function liveUsageSeconds(state) {
    if (state.onSec === null) return 0;
    const end = state.offSec !== null ? state.offSec : state.lastSec;
    return Math.max(0, end - state.onSec);
  }

  function liveTimeLabel(state, sec) {
    const base = state.baseSeconds === null ? startSeconds - 1 : state.baseSeconds;
    return formatClock(base + sec);
  }

  function liveViewEnd(state) {
    return LIVE_VIEW_TO;
  }

  function viewRange(focus, seconds = DEFAULT_VIEW_SECONDS) {
    const width = Math.max(1, Math.min(TOTAL_SECONDS, seconds));
    const from = Math.max(0, Math.min(TOTAL_SECONDS - width, Math.round(focus) - width + 15));
    return { from, to: from + width };
  }

  function visibleTicks(from, to) {
    const ticks = [from, to];
    for (let second = Math.ceil(from / 30) * 30; second <= to; second += 30) ticks.push(second);
    return [...new Set(ticks)];
  }

  function updateSharedView(focus, force = false) {
    viewFocus = Math.max(0, Math.min(TOTAL_SECONDS, focus));
    const next = viewRange(viewFocus, viewSeconds);
    if (!force && next.from === viewFrom) return;
    viewFrom = next.from;
    const label = document.getElementById('viewRangeLabel');
    if (label) label.textContent = `${formatTime(viewFrom)}~${formatTime(next.to)}`;
    if (replayRedraw) replayRedraw();
    if (liveRedraw) liveRedraw();
  }

  // 실시간 표본 번호는 평소 기록의 초 오프셋보다 1 크다.
  function liveTicks(end) {
    return visibleTicks(viewFrom, viewFrom + viewSeconds).map(second => second + 1);
  }

  function liveStatusText(state) {
    if (state.lastSec >= LIVE_TOTAL || state.status === 'completed') return `MQTT 발행 ${LIVE_TOTAL}건 완료 · 전자레인지 미사용`;
    return `전자레인지 미사용 · 대기전력 발행 중 (${state.lastSec}/${LIVE_TOTAL})`;
  }

  function isUnsupportedDemoScenario(error) {
    return error.message.includes('지원하지 않는 시나리오') && error.message.includes(LIVE_SCENARIO);
  }

  // ---- 그리기 ----
  function getCanvasMetrics(canvas) {
    const ratio = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(320, Math.round(rect.width));
    const height = Math.max(120, Math.round(rect.height));
    if (canvas.width !== Math.round(width * ratio) || canvas.height !== Math.round(height * ratio)) {
      canvas.width = Math.round(width * ratio);
      canvas.height = Math.round(height * ratio);
    }
    const context = canvas.getContext('2d');
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    return { context, width, height };
  }

  function drawFrame(canvas, from, to, timeTicks, labelFor, maxPower = POWER_MAX) {
    const { context: ctx, width, height } = getCanvasMetrics(canvas);
    const pad = { left: 46, right: 16, top: 12, bottom: 24 };
    const plotWidth = width - pad.left - pad.right;
    const plotHeight = height - pad.top - pad.bottom;
    const xAt = index => pad.left + ((index - from) / (to - from)) * plotWidth;
    const yAt = power => pad.top + (1 - Math.min(maxPower, Math.max(0, power)) / maxPower) * plotHeight;

    ctx.clearRect(0, 0, width, height);
    ctx.font = '10px Pretendard, -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    [0, maxPower / 3, maxPower * 2 / 3, maxPower].forEach(power => {
      const y = yAt(power);
      ctx.strokeStyle = COLORS.grid;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(width - pad.right, y);
      ctx.stroke();
      ctx.fillStyle = COLORS.tick;
      ctx.fillText(maxPower === POWER_MAX && power === POWER_MAX ? '1.2kW' : String(Math.round(power)), pad.left - 8, y);
    });

    // 눈금은 주어진 순서(우선순위)대로 그리고, 이미 그린 눈금과 겹치면 뺀다. 양 끝 눈금은 안쪽으로 정렬한다.
    const drawn = [];
    timeTicks.forEach(second => {
      if (second < from || second > to) return;
      const label = labelFor(second);
      const textWidth = ctx.measureText(label).width;
      const x = xAt(second);
      const left = second === from ? x : second === to ? x - textWidth : x - textWidth / 2;
      const right = left + textWidth;
      if (drawn.some(span => left < span.right + 8 && right > span.left - 8)) return;
      drawn.push({ left, right });
      ctx.fillStyle = COLORS.tick;
      ctx.textAlign = 'left';
      ctx.textBaseline = 'bottom';
      ctx.fillText(label, left, height - 2);
    });

    return { ctx, pad, plotHeight, xAt, yAt, from, to };
  }

  function plotLine(frame, line, color, limit) {
    const { ctx, xAt, yAt, from, to } = frame;
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 2.25;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.beginPath();
    let penDown = false;
    for (let index = from; index <= Math.min(limit, to); index += 1) {
      const value = line[index];
      if (value === null || value === undefined || Number.isNaN(value)) { penDown = false; continue; }
      if (penDown) ctx.lineTo(xAt(index), yAt(value)); else ctx.moveTo(xAt(index), yAt(value));
      penDown = true;
    }
    ctx.stroke();
    ctx.restore();
  }

  function drawCursor(frame, x, y, color) {
    const { ctx, pad, plotHeight } = frame;
    ctx.strokeStyle = color;
    ctx.globalAlpha = .38;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x, pad.top);
    ctx.lineTo(x, pad.top + plotHeight);
    ctx.stroke();
    ctx.globalAlpha = 1;
    if (y === null) return;
    ctx.fillStyle = '#ffffff';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
  }

  function drawBand(frame, from, to) {
    const { ctx, pad, plotHeight, xAt } = frame;
    from = Math.max(from, frame.from);
    to = Math.min(to, frame.to);
    if (to <= from) return;
    ctx.fillStyle = COLORS.band;
    ctx.fillRect(xAt(from), pad.top, xAt(to) - xAt(from), plotHeight);
  }

  function drawMarker(frame, at) {
    const { ctx, pad, plotHeight, xAt } = frame;
    if (at < frame.from || at > frame.to) return;
    ctx.save();
    ctx.strokeStyle = COLORS.marker;
    ctx.globalAlpha = .7;
    ctx.setLineDash([4, 4]);
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(xAt(at), pad.top);
    ctx.lineTo(xAt(at), pad.top + plotHeight);
    ctx.stroke();
    ctx.restore();
  }

  function drawReplay(canvas, data, position) {
    const to = viewFrom + viewSeconds;
    const frame = drawFrame(canvas, viewFrom, to, visibleTicks(viewFrom, to), formatTime);
    const { xAt, yAt } = frame;
    drawBand(frame, USE_START, USE_END);
    drawMarker(frame, LIMIT_AT);
    plotLine(frame, data, COLORS.normal, TOTAL_SECONDS);
    if (position >= viewFrom && position <= to) drawCursor(frame, xAt(position), yAt(data[position]), COLORS.normal);
  }

  function drawLive(canvas, state) {
    const from = viewFrom + 1;
    const end = viewFrom + viewSeconds + 1;
    const frame = drawFrame(canvas, from, end, liveTicks(end), sec => liveTimeLabel(state, sec));
    const { xAt, yAt } = frame;
    drawBand(frame, USE_START + 1, USE_END + 1);
    drawMarker(frame, 299);
    plotLine(frame, state.power, COLORS.anomaly, state.lastSec);
    if (state.lastSec >= from && state.lastSec <= end) {
      const value = state.power[state.lastSec];
      drawCursor(frame, xAt(state.lastSec), value === null ? null : yAt(value), COLORS.anomaly);
    }
  }

  // ---- 서버 연동 ----
  async function requestJson(method, url, body) {
    const response = await fetch(url, {
      method,
      headers: body ? { 'Content-Type': 'application/json' } : undefined,
      body: body ? JSON.stringify(body) : undefined
    });
    let payload = null;
    try { payload = await response.json(); } catch (err) { payload = null; }
    if (!response.ok) {
      const message = payload && payload.message ? payload.message : `HTTP ${response.status}`;
      throw new Error(message);
    }
    return payload || {};
  }

  function initLive() {
    const canvas = document.getElementById('anomalyChart');
    const el = {
      card: document.getElementById('liveCard'),
      badge: document.getElementById('liveBadge'),
      clock: document.getElementById('liveClock'),
      power: document.getElementById('livePower'),
      progress: document.getElementById('liveProgress'),
      start: document.getElementById('liveStartButton'),
      stop: document.getElementById('liveStopButton'),
      speed: document.getElementById('liveSpeedSelect'),
      message: document.getElementById('liveMessage'),
      usage: document.getElementById('liveUsage')
    };
    let state = createLiveState();
    let phase = 'idle'; // idle | starting | running | completed | stopped | error
    let stream = null;
    let watchdog = null;
    let stoppingForCompletion = false;

    function setPhase(next, message) {
      phase = next;
      el.card.dataset.state = next;
      const busy = next === 'starting' || next === 'running';
      el.start.disabled = busy;
      el.stop.disabled = !busy;
      el.speed.disabled = busy;
      el.start.textContent = next === 'completed' || next === 'stopped' || next === 'error' ? '↻ 다시 실행' : '▶ 실시간 실행';
      const badge = {
        idle: ['실행 전', 'status-waiting'],
        starting: ['연결 중', 'status-active'],
        running: ['발행 중', 'status-active'],
        completed: ['발행 완료', 'status-alert'],
        stopped: ['중지됨', 'status-waiting'],
        error: ['실행 실패', 'status-alert']
      }[next];
      el.badge.textContent = badge[0];
      el.badge.className = 'status-badge ' + badge[1];
      if (message) el.message.textContent = message;
      if (next === 'running' && !watchdog) watchdog = window.setInterval(checkServer, state.scenario === FALLBACK_SCENARIO ? 500 : 3000);
      if (next !== 'running' && watchdog) { window.clearInterval(watchdog); watchdog = null; }
    }

    function finish() {
      closeStream();
      setPhase('completed', `${liveTimeLabel(state, 1)}부터 대기전력 ${LIVE_TOTAL}건을 MQTT로 발행했습니다. 전자레인지는 사용되지 않았습니다. Kafka 적재와 AI 판정 결과는 브리지·분석 서비스에서 확인합니다.`);
    }

    async function finishFallback() {
      if (stoppingForCompletion) return;
      stoppingForCompletion = true;
      closeStream();
      setPhase('starting', '비교 구간이 끝나 발행을 종료하는 중입니다.');
      try {
        await requestJson('POST', 'api/stop');
        const status = await requestJson('GET', 'api/status');
        const house = status.active_households && status.active_households[LIVE_HOUSE];
        const published = house && Number.isInteger(house.cycle_count) ? house.cycle_count : state.lastSec;
        setPhase('completed', `${formatTime(0)}~${formatTime(TOTAL_SECONDS)} 비교 구간을 표시했습니다. MQTT 발행 ${published}건 · 전자레인지 미사용. Kafka 적재와 AI 판정 결과는 브리지·분석 서비스에서 확인합니다.`);
      } catch (err) {
        setPhase('error', `비교 구간 발행 종료에 실패했습니다: ${err.message}`);
      }
      render();
    }

    // SSE 큐가 가득 차면 서버가 표본을 버리고, 다른 화면에서 중지하면 알림이 오지 않으므로 상태를 주기적으로 대조한다.
    async function checkServer() {
      if (phase !== 'running') return;
      try {
        const status = await requestJson('GET', 'api/status');
        const house = status.active_households && status.active_households[LIVE_HOUSE];
        if (house && house.scenario === state.scenario && house.status === 'running') {
          if (state.scenario === FALLBACK_SCENARIO && house.cycle_count >= LIVE_TOTAL) await finishFallback();
          return;
        }
        if (phase !== 'running') return;
        if (house && house.scenario === state.scenario && house.status === 'completed') {
          const last = status.last_metrics_by_house && status.last_metrics_by_house[LIVE_HOUSE];
          if (last && applyLiveSample(state, last) && state.baseSeconds !== null) {
            setStartSeconds(state.baseSeconds + 1);
            refreshTimeCopy();
          }
          updateSharedView(state.lastSec - 1);
          finish();
        } else {
          closeStream();
          setPhase('stopped', `${state.lastSec}건 발행 후 실행이 종료되었습니다. 다른 화면에서 중지했거나 다른 시나리오가 시작되었습니다.`);
        }
        render();
      } catch (err) { /* 일시적인 조회 실패는 다음 주기에 다시 확인한다 */ }
    }

    function render() {
      drawLive(canvas, state);
      el.clock.textContent = state.simTime || '--:--:--';
      const value = state.power[state.lastSec];
      el.power.textContent = state.lastSec > 0 && value !== null ? Math.round(value).toLocaleString('ko-KR') : '—';
      el.progress.textContent = `발행 ${state.lastSec} / ${LIVE_TOTAL}`;
      el.usage.textContent = state.onSec === null ? '미사용' : formatDuration(liveUsageSeconds(state));
      if (phase === 'running') {
        el.badge.textContent = '전자레인지 미사용';
        el.badge.className = 'status-badge status-active';
      }
    }
    liveRedraw = render;

    function closeStream() {
      if (stream) { stream.close(); stream = null; }
    }

    function openStream() {
      closeStream();
      stream = new EventSource('api/stream');
      stream.onmessage = function (event) {
        let sample;
        try { sample = JSON.parse(event.data); } catch (err) { return; }
        if (phase !== 'running' && phase !== 'starting') return;
        const hadBase = state.baseSeconds !== null;
        if (!applyLiveSample(state, sample)) return;
        if (!hadBase && state.baseSeconds !== null) {
          setStartSeconds(state.baseSeconds + 1);
          refreshTimeCopy();
        }
        updateSharedView(state.lastSec - 1);
        if (state.status === 'completed' || state.lastSec >= LIVE_TOTAL) {
          if (state.scenario === FALLBACK_SCENARIO) void finishFallback();
          else finish();
        } else if (phase === 'running') {
          el.message.textContent = liveStatusText(state);
        }
        render();
      };
    }

    async function start() {
      setPhase('starting', '시뮬레이터 상태를 확인하는 중입니다.');
      try {
        const status = await requestJson('GET', 'api/status');
        if (status.is_running) {
          const ok = window.confirm(`시뮬레이터에서 '${status.current_mode || '다른'}' 실행이 진행 중입니다.\n중지하고 H001 아침 미사용 전력 발행을 시작할까요?`);
          if (!ok) { setPhase('idle', '실행을 취소했습니다. 진행 중인 시뮬레이션은 그대로 유지됩니다.'); return; }
        }
        state = createLiveState();
        stoppingForCompletion = false;
        setStartSeconds(currentKstSeconds());
        refreshTimeCopy();
        updateSharedView(0, true);
        render();
        openStream();
        const speed = Number(el.speed.value) || 1;
        const interval = Math.round(1000 / speed) / 1000;
        try {
          await requestJson('POST', 'api/start', { house: LIVE_HOUSE, scenario: LIVE_SCENARIO, interval });
          setPhase('running', `발행 시작 · 첫 측정값의 현재 시각에 맞춰 두 그래프를 표시합니다.`);
        } catch (err) {
          if (!isUnsupportedDemoScenario(err)) throw err;
          state.scenario = FALLBACK_SCENARIO;
          await requestJson('POST', 'api/start', { house: LIVE_HOUSE, scenario: FALLBACK_SCENARIO, interval });
          setPhase('running', '기존 서버 호환 방식으로 현재 시각부터 대기전력을 발행 중입니다.');
        }
      } catch (err) {
        closeStream();
        setPhase('error', `실행하지 못했습니다: ${err.message}`);
      }
    }

    async function stop() {
      el.stop.disabled = true;
      closeStream();
      try {
        await requestJson('POST', 'api/stop');
        setPhase('stopped', `${state.lastSec}건 발행 후 중지했습니다.`);
      } catch (err) {
        setPhase('error', `중지 요청이 실패했습니다: ${err.message}`);
      }
      render();
    }

    // 시연 전용 시나리오가 진행 중이면 첫 표본 시각에 맞춰 다시 연결한다.
    async function attachIfRunning() {
      try {
        const status = await requestJson('GET', 'api/status');
        const house = status.active_households && status.active_households[LIVE_HOUSE];
        if (!house || house.status !== 'running') return;
        if (house.scenario !== LIVE_SCENARIO) return;
        state = createLiveState(house.scenario);
        const last = status.last_metrics_by_house && status.last_metrics_by_house[LIVE_HOUSE];
        if (last && applyLiveSample(state, last) && state.baseSeconds !== null) {
          setStartSeconds(state.baseSeconds + 1);
          refreshTimeCopy();
        }
        updateSharedView(state.lastSec - 1);
        setPhase('running', '진행 중인 실행에 연결했습니다. 연결 이전 구간은 비어 있습니다.');
        openStream();
        render();
      } catch (err) { /* 상태 조회 실패 시 실행 전 상태로 둔다 */ }
    }

    el.start.addEventListener('click', start);
    el.stop.addEventListener('click', stop);
    // SSE는 브라우저 연결 풀을 점유하므로 페이지를 떠날 때 닫는다. 다른 탭으로 전환하는 동안에는 유지한다.
    window.addEventListener('pagehide', function () {
      closeStream();
      if (state.scenario === FALLBACK_SCENARIO && (phase === 'running' || phase === 'starting') && navigator.sendBeacon) {
        navigator.sendBeacon('api/stop', new Blob(['{}'], { type: 'application/json' }));
      }
    });
    window.addEventListener('pageshow', function (event) {
      if (event.persisted && phase === 'running') openStream();
    });
    window.addEventListener('resize', render);
    setPhase('idle');
    render();
    attachIfRunning();
  }

  function initReplay() {
    const canvas = document.getElementById('normalChart');
    const series = createSeries();
    const elements = {
      clock: document.getElementById('demoClock'),
      normalPower: document.getElementById('normalPower'),
      normalBadge: document.getElementById('normalBadge'),
      play: document.getElementById('playButton'),
      reset: document.getElementById('resetButton'),
      speed: document.getElementById('speedSelect'),
      scrubber: document.getElementById('demoScrubber'),
      storyIndex: document.getElementById('storyIndex'),
      storyEyebrow: document.getElementById('storyEyebrow'),
      storyTitle: document.getElementById('storyTitle'),
      storyBody: document.getElementById('storyBody')
    };
    let position = 0;
    let playing = false;
    let lastTick = 0;
    let accumulator = 0;

    function render() {
      drawReplay(canvas, series.normal, position);
      elements.clock.textContent = formatTime(position);
      elements.normalPower.textContent = Math.round(series.normal[position]).toLocaleString('ko-KR');
      elements.scrubber.value = String(position);

      const usingAppliance = position >= USE_START && position < USE_END;
      elements.normalBadge.textContent = usingAppliance ? '전자레인지 사용 중' : position >= USE_END ? '사용 종료' : '대기전력';
      elements.normalBadge.className = 'status-badge ' + (usingAppliance ? 'status-active' : 'status-normal');

      const story = storyFor(position);
      elements.storyIndex.textContent = story.index;
      elements.storyEyebrow.textContent = story.eyebrow;
      elements.storyTitle.textContent = story.title;
      elements.storyBody.textContent = story.body;
    }
    replayRedraw = render;

    function setPlaying(next) {
      playing = next;
      lastTick = 0;
      accumulator = 0;
      elements.play.textContent = playing ? 'Ⅱ 일시정지' : position >= TOTAL_SECONDS ? '↻ 다시 재생' : '▶ 재생';
      if (playing) window.requestAnimationFrame(tick);
    }

    function tick(timestamp) {
      if (!playing) return;
      if (!lastTick) lastTick = timestamp;
      const elapsed = Math.min(250, timestamp - lastTick);
      lastTick = timestamp;
      accumulator += elapsed * Number(elements.speed.value) / 1000;
      const advance = Math.floor(accumulator);
      if (advance > 0) {
        accumulator -= advance;
        position = Math.min(TOTAL_SECONDS, position + advance);
        updateSharedView(position);
        render();
      }
      if (position >= TOTAL_SECONDS) setPlaying(false);
      else window.requestAnimationFrame(tick);
    }

    elements.play.addEventListener('click', function () {
      if (!playing && position >= TOTAL_SECONDS) position = 0;
      setPlaying(!playing);
      updateSharedView(position);
      render();
    });
    elements.reset.addEventListener('click', function () {
      setPlaying(false);
      position = 0;
      updateSharedView(position);
      render();
    });
    elements.scrubber.addEventListener('input', function () {
      setPlaying(false);
      position = Number(elements.scrubber.value);
      updateSharedView(position);
      render();
    });
    window.addEventListener('resize', render);
    render();
  }

  function init() {
    if (!document.getElementById('normalChart') || !document.getElementById('anomalyChart')) return;
    initReplay();
    initLive();
    const viewSelect = document.getElementById('viewWindowSelect');
    viewSelect.addEventListener('change', function () {
      viewSeconds = Number(viewSelect.value) || DEFAULT_VIEW_SECONDS;
      updateSharedView(viewFocus, true);
    });
    refreshTimeCopy();
  }

  window.RoutineDemo = {
    createSeries, formatTime, storyFor, setStartSeconds, currentKstSeconds,
    createLiveState, applyLiveSample, liveTimeLabel, liveViewEnd, liveTicks, liveStatusText, liveUsageSeconds, formatDuration, isUnsupportedDemoScenario, viewRange, visibleTicks,
    constants: { TOTAL_SECONDS, USE_START, USE_END, LIMIT_AT, LIVE_TOTAL, LIVE_VIEW_FROM, LIVE_VIEW_TO, USUAL_USE_SEC, LIVE_HOUSE, LIVE_SCENARIO, FALLBACK_SCENARIO, LIVE_APPLIANCE, DEFAULT_VIEW_SECONDS }
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
}());
