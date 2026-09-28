// The live SSE is emitted only after MQTT publication has settled. It is not
// evidence that the analysis server received or processed the sample. The
// diagram intentionally ends at the broker.
let powerflowGeneration = 0;
let powerflowTimers = [];
let powerflowRunId = null;
let powerflowHouse = null;
// 발행량 요약용 집계. 패널의 주 상태(data-visual-state)는 여전히 이 숫자와 무관하게 결정된다.
let powerflowPublished = 0;
let powerflowPublishTimes = [];
const POWERFLOW_RATE_WINDOW_MS = 3000;

function powerflowElement(id) {
  return document.getElementById(id);
}

function powerflowAnnounce(message) {
  const full = `${message}. MQTT 브로커 발행 확인까지 표시`;
  const panel = powerflowElement('powerflowPanel');
  if (panel) panel.setAttribute('aria-label', `계측 데이터 전송 흐름: ${full}`);
  const live = powerflowElement('powerflowA11yStatus');
  if (live && live.textContent !== full) live.textContent = full;
}

function powerflowLater(callback, delay) {
  const id = setTimeout(() => {
    powerflowTimers = powerflowTimers.filter(timer => timer !== id);
    callback();
  }, delay);
  powerflowTimers.push(id);
}

function powerflowClearMotion() {
  powerflowGeneration += 1;
  powerflowTimers.forEach(clearTimeout);
  powerflowTimers = [];
  const lane = powerflowElement('powerflowMqttLane');
  if (lane) {
    Array.from(lane.children).forEach(child => {
      if (child.className === 'flow-packet') lane.removeChild(child);
    });
  }
  const board = powerflowElement('powerflowBoard');
  const broker = powerflowElement('powerflowBroker');
  if (board) board.className = 'flow-device flow-board';
  if (broker) broker.className = 'flow-device flow-broker';
}

function powerflowReducedMotion() {
  return !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
}

function powerflowRenderThroughput() {
  const total = powerflowElement('powerflowTotalCount');
  if (total) total.textContent = powerflowPublished.toLocaleString();
  const rate = powerflowElement('powerflowRate');
  if (rate) rate.textContent = (powerflowPublishTimes.length / (POWERFLOW_RATE_WINDOW_MS / 1000)).toFixed(1);
}

// 발행이 멈추면 창이 비면서 속도가 자연스럽게 0으로 내려간다.
function powerflowTrimPublishTimes(now) {
  powerflowPublishTimes = powerflowPublishTimes.filter(time => now - time <= POWERFLOW_RATE_WINDOW_MS);
}

function powerflowCountPublish() {
  const now = Date.now();
  powerflowPublished += 1;
  powerflowPublishTimes.push(now);
  powerflowTrimPublishTimes(now);
  powerflowRenderThroughput();
}

function powerflowResetThroughput() {
  powerflowPublished = 0;
  powerflowPublishTimes = [];
  powerflowRenderThroughput();
}

function powerflowSetRing(percent) {
  const ring = powerflowElement('powerflowBurstRing');
  if (!ring) return;
  if (ring.style.setProperty) ring.style.setProperty('--burst-progress', percent);
  else ring.style['--burst-progress'] = percent;
}

function powerflowSetBurstGauge(published, planned) {
  const pub = Math.max(0, Number(published) || 0);
  const plan = Math.max(0, Number(planned) || 0);
  const percent = plan > 0 ? Math.min(100, pub / plan * 100) : 0;
  powerflowSetRing(`${percent}%`);
  const gauge = powerflowElement('powerflowBurst');
  if (gauge) gauge.setAttribute('aria-label', `MQTT 발행 ${pub}건 / 계획 ${plan}건`);
  const count = powerflowElement('powerflowBurstCount');
  if (count) count.textContent = `${pub.toLocaleString()}건 / ${plan.toLocaleString()}건`;
  const ratio = powerflowElement('powerflowBurstPercent');
  if (ratio) ratio.textContent = `${percent.toFixed(1)}%`;
}

function powerflowPulse() {
  const panel = powerflowElement('powerflowPanel');
  const lane = powerflowElement('powerflowMqttLane');
  if (!panel || !lane || powerflowReducedMotion()) return;
  const generation = powerflowGeneration;
  const board = powerflowElement('powerflowBoard');
  const broker = powerflowElement('powerflowBroker');
  const packet = document.createElement('span');
  packet.className = 'flow-packet';
  packet.setAttribute('aria-hidden', 'true');
  lane.appendChild(packet);
  const packets = Array.from(lane.children).filter(child => child.className === 'flow-packet');
  if (packets.length > 8) lane.removeChild(packets[0]);
  if (board) board.className = 'flow-device flow-board flow-react';
  powerflowLater(() => {
    if (generation === powerflowGeneration && board) board.className = 'flow-device flow-board';
  }, 500);
  powerflowLater(() => {
    if (generation === powerflowGeneration && broker) {
      broker.className = 'flow-device flow-broker';
      void broker.offsetWidth;
      broker.className = 'flow-device flow-broker flow-react';
    }
  }, 800);
  powerflowLater(() => {
    if (generation !== powerflowGeneration) return;
    if (packet.parentNode === lane) lane.removeChild(packet);
    if (broker) broker.className = 'flow-device flow-broker';
  }, 1150);
}

function powerflowSetMode(mode) {
  const panel = powerflowElement('powerflowPanel');
  if (!panel) return;
  if (panel.getAttribute('data-mode') !== mode) powerflowClearMotion();
  panel.setAttribute('data-mode', mode === 'burst' ? 'burst' : 'live');
}

function powerflowSetRunState(state) {
  const panel = powerflowElement('powerflowPanel');
  if (!panel) return;
  const next = ['running', 'paused', 'completed'].includes(state) ? state : 'idle';
  const previous = panel.getAttribute('data-run-state');
  panel.setAttribute('data-run-state', next);
  if (next !== 'running') {
    powerflowClearMotion();
    panel.setAttribute('data-confirmed', '0');
  }
  if (next === 'paused') {
    panel.setAttribute('data-visual-state', 'paused');
    powerflowAnnounce('일시정지');
  } else if (next === 'completed') {
    panel.setAttribute('data-visual-state', 'completed');
    powerflowAnnounce('종료');
  } else if (next === 'idle') {
    panel.setAttribute('data-visual-state', 'idle');
    panel.setAttribute('data-confirmed', '0');
    powerflowAnnounce('대기');
  } else if (previous !== 'running' || panel.getAttribute('data-visual-state') !== 'confirmed') {
    panel.setAttribute('data-confirmed', '0');
    panel.setAttribute('data-visual-state', 'waiting');
    powerflowAnnounce('발행 대기');
  }
}

function powerflowReset() {
  powerflowClearMotion();
  powerflowRunId = null;
  powerflowHouse = null;
  const panel = powerflowElement('powerflowPanel');
  if (!panel) return;
  panel.setAttribute('data-mode', 'live');
  panel.setAttribute('data-fault', '0');
  panel.setAttribute('data-confirmed', '0');
  panel.setAttribute('data-visual-state', 'idle');
  panel.setAttribute('data-run-state', 'idle');
  powerflowSetBurstGauge(0, 0);
  powerflowResetThroughput();
  powerflowAnnounce('대기');
}

function powerflowBeginRun(runId) {
  powerflowReset();
  powerflowRunId = runId || null;
  powerflowSetRunState('running');
}

function powerflowSetObservedHouse(house) {
  powerflowHouse = house;
  powerflowClearMotion();
  const panel = powerflowElement('powerflowPanel');
  if (!panel) return;
  panel.setAttribute('data-confirmed', '0');
  panel.setAttribute('data-fault', '0');
  powerflowSetBurstGauge(0, 0);
  powerflowResetThroughput();
  if (panel.getAttribute('data-run-state') === 'running') {
    panel.setAttribute('data-visual-state', 'waiting');
    powerflowAnnounce('발행 대기');
  }
}

function powerflowApplyRealtime(metric) {
  const panel = powerflowElement('powerflowPanel');
  if (!panel || panel.getAttribute('data-mode') !== 'live') return;
  if (metric.house !== powerflowHouse && powerflowHouse !== null) return;
  if (metric.source === 'E2E') {
    if (!powerflowRunId || metric.run_id !== powerflowRunId) return;
  } else if (powerflowRunId) {
    return;
  }
  if (metric.status === 'completed' || metric.status === 'stopped') {
    powerflowSetRunState('completed');
    return;
  }
  if (panel.getAttribute('data-run-state') !== 'running') return;
  const missing = metric.sensorFault || metric.measurementAvailable === false;
  const published = !missing && metric.status === 'running' &&
    (metric.source !== 'E2E' || metric.measurementAvailable === true) &&
    typeof metric.totalP === 'number' && Number.isFinite(metric.totalP);
  if (!published) {
    powerflowTrimPublishTimes(Date.now());
    powerflowRenderThroughput();
    powerflowClearMotion();
    panel.setAttribute('data-fault', missing ? '1' : '0');
    panel.setAttribute('data-confirmed', '0');
    panel.setAttribute('data-visual-state', missing ? 'missing' : 'waiting');
    powerflowAnnounce(missing ? '계측 결측, 발행 중단' : '발행 대기');
    return;
  }
  panel.setAttribute('data-fault', '0');
  panel.setAttribute('data-confirmed', '1');
  panel.setAttribute('data-visual-state', 'confirmed');
  powerflowAnnounce('MQTT 브로커 발행 확인');
  powerflowCountPublish();
  powerflowPulse();
}

function powerflowRenderE2E(snapshot) {
  const panel = powerflowElement('powerflowPanel');
  if (!panel || !snapshot) return;
  const runId = snapshot.run_id || null;
  if (runId !== powerflowRunId) powerflowBeginRun(runId);
  const burst = snapshot.execution_mode === 'BURST';
  powerflowSetMode(burst ? 'burst' : 'live');
  const status = snapshot.overall_status || snapshot.overall_state || 'IDLE';
  const house = (snapshot.households || []).find(item => item.household_id === (powerflowHouse || observedHouse));
  const visualStatus = house && house.state ? house.state : status;
  const state = ['RUNNING', 'STARTING'].includes(visualStatus) ? 'running' :
    ['PAUSED', 'PAUSING', 'STOPPING'].includes(visualStatus) ? 'paused' :
      ['COMPLETED', 'STOPPED', 'FAILED', 'PARTIAL_FAILED'].includes(visualStatus) ? 'completed' : 'idle';
  powerflowSetRunState(state);
  if (!burst) return;
  const published = Math.max(0, Number(house && house.published_samples) || 0);
  const planned = Math.max(0, Number(house && house.planned_publish_samples) || 0);
  powerflowSetBurstGauge(published, planned);
  panel.setAttribute('data-confirmed', state === 'running' && published > 0 ? '1' : '0');
  if (state === 'running') {
    panel.setAttribute('data-visual-state', published > 0 ? 'confirmed' : 'waiting');
    powerflowAnnounce(`MQTT 발행 ${published}건 / 계획 ${planned}건`);
  }
}
