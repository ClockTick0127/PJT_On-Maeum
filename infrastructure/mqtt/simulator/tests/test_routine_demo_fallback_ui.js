const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const code = fs.readFileSync(path.join(__dirname, '..', 'assets', 'routine_demo.js'), 'utf8');
const calls = [];
const elements = new Map();
const canvasContext = new Proxy({ measureText: () => ({ width: 55 }) }, {
  get(target, key) { return target[key] === undefined ? () => {} : target[key]; }
});

function element(id) {
  if (!elements.has(id)) {
    elements.set(id, {
      dataset: {}, textContent: '', disabled: false, value: id === 'liveSpeedSelect' ? '10' : '0',
      listeners: {}, className: '',
      addEventListener(name, handler) { this.listeners[name] = handler; },
      setAttribute(name, value) { this[name] = value; },
      getBoundingClientRect() { return { width: 600, height: 180 }; },
      getContext() { return canvasContext; }
    });
  }
  return elements.get(id);
}

let stream;
class EventSourceMock {
  constructor() { stream = this; }
  close() { this.closed = true; }
}

function response(ok, payload, status = ok ? 200 : 400) {
  return { ok, status, json: async () => payload };
}

let statusReads = 0;
async function fetchMock(url, options) {
  const method = options.method;
  const body = options.body ? JSON.parse(options.body) : null;
  calls.push({ method, url, body });
  if (url === 'api/status') {
    statusReads += 1;
    if (statusReads < 3) return response(true, { is_running: false });
    return response(true, { active_households: { H001: { scenario: 'manual', status: 'stopped', cycle_count: 330 } } });
  }
  if (url === 'api/start' && body.scenario === 'routine_missed_demo') {
    return response(false, { message: "지원하지 않는 시나리오입니다: 'routine_missed_demo'. 허용 목록: ['manual']" });
  }
  if (url === 'api/start' && body.scenario === 'manual') return response(true, { status: 'started' });
  if (url === 'api/stop') return response(true, { status: 'stopped' });
  throw new Error(`예상하지 못한 요청: ${method} ${url}`);
}

const context = {
  Date: class extends Date { static now() { return Date.parse('2026-09-26T03:34:56Z'); } },
  fetch: fetchMock,
  EventSource: EventSourceMock,
  document: { readyState: 'complete', getElementById: element },
  window: {
    devicePixelRatio: 1,
    setInterval: () => 1,
    clearInterval() {},
    addEventListener() {},
    requestAnimationFrame() {}
  }
};
vm.createContext(context);
vm.runInContext(code, context);
assert.strictEqual(element('viewRangeLabel').textContent, '12:34:56~12:36:56');
element('viewWindowSelect').value = '329';
element('viewWindowSelect').listeners.change();
assert.strictEqual(element('viewRangeLabel').textContent, '12:34:56~12:40:25');
element('viewWindowSelect').value = '120';
element('viewWindowSelect').listeners.change();
element('demoScrubber').value = '200';
element('demoScrubber').listeners.input();
assert.strictEqual(element('viewRangeLabel').textContent, '12:36:31~12:38:31');

(async () => {
  await Promise.resolve(); // 초기 실행 상태 조회
  await element('liveStartButton').listeners.click();

  const starts = calls.filter(call => call.url === 'api/start');
  assert.deepStrictEqual(starts.map(call => call.body.scenario), ['routine_missed_demo', 'manual']);
  assert.strictEqual(starts[1].body.start_time, undefined);
  assert.strictEqual(starts[1].body.interval, 0.1);
  assert.strictEqual(element('liveCard').dataset.state, 'running');

  stream.onmessage({ data: JSON.stringify({
    house: 'H001', scenario: 'manual', sec: 330, status: 'running',
    totalP: 56, measurementAvailable: true,
    simTimeKst: '12:40:30', simDateKst: '2026-09-26',
    devices: { microwave: { enabled: false } }
  }) });
  for (let index = 0; index < 5 && element('liveCard').dataset.state !== 'completed'; index += 1) {
    await new Promise(resolve => setImmediate(resolve));
  }

  assert.strictEqual(calls.filter(call => call.url === 'api/stop').length, 1);
  assert.strictEqual(element('liveCard').dataset.state, 'completed');
  assert.strictEqual(element('liveProgress').textContent, '발행 330 / 330');
  assert.strictEqual(element('rangeStartLabel').textContent, '12:35:01');
  assert.strictEqual(element('rangeEndLabel').textContent, '12:40:30');
  assert.strictEqual(element('viewRangeLabel').textContent, '12:38:30~12:40:30');
  console.log('routine demo fallback UI tests passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
