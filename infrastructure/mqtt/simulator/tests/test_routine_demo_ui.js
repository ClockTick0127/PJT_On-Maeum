const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const code = fs.readFileSync(path.join(__dirname, '..', 'assets', 'routine_demo.js'), 'utf8');
const FixedDate = class extends Date {
  static now() { return Date.parse('2026-09-26T03:34:56Z'); }
};
const context = { Date: FixedDate, window: {}, document: { readyState: 'loading', addEventListener() {} } };
vm.createContext(context);
vm.runInContext(code, context);
const demo = context.window.RoutineDemo;
assert.ok(demo);

// 현재 KST를 첫 시각으로 사용하고, 평소 사용 파형은 실행 시각에 맞춰 이동한다.
const series = demo.createSeries().normal;
assert.strictEqual(series.length, 330);
assert.ok(series.slice(0, 30).every(value => value < 100));
assert.ok(series[30] > 1200);
assert.ok(series.slice(30, 90).every(value => value > 900));
assert.ok(series.slice(90).every(value => value < 100));
assert.strictEqual(demo.currentKstSeconds(), 12 * 3600 + 34 * 60 + 56);
assert.strictEqual(demo.formatTime(0), '12:34:56');
assert.strictEqual(demo.formatTime(30), '12:35:26');
assert.strictEqual(demo.formatTime(90), '12:36:26');
assert.strictEqual(demo.formatTime(329), '12:40:25');
assert.strictEqual(demo.storyFor(90).index, '03 / 03');
demo.setStartSeconds(23 * 3600 + 59 * 60 + 30);
assert.strictEqual(demo.formatTime(329), '00:04:59');
demo.setStartSeconds(12 * 3600 + 34 * 60 + 56);

// 두 그래프는 같은 시각과 같은 330개 표본 구간을 사용한다.
assert.strictEqual(demo.constants.LIVE_HOUSE, 'H001');
assert.strictEqual(demo.constants.LIVE_SCENARIO, 'routine_missed_demo');
assert.strictEqual(demo.constants.FALLBACK_SCENARIO, 'manual');
assert.strictEqual(demo.constants.LIVE_TOTAL, 330);
assert.strictEqual(demo.constants.DEFAULT_VIEW_SECONDS, 120);
assert.deepStrictEqual(JSON.parse(JSON.stringify(demo.viewRange(0))), { from: 0, to: 120 });
assert.deepStrictEqual(JSON.parse(JSON.stringify(demo.viewRange(120))), { from: 15, to: 135 });
assert.deepStrictEqual(JSON.parse(JSON.stringify(demo.viewRange(329))), { from: 209, to: 329 });
assert.deepStrictEqual(JSON.parse(JSON.stringify(demo.viewRange(329, 329))), { from: 0, to: 329 });
assert.deepStrictEqual(Array.from(demo.visibleTicks(0, 120)).sort((a, b) => a - b), [0, 30, 60, 90, 120]);
assert.strictEqual(demo.isUnsupportedDemoScenario(new Error("지원하지 않는 시나리오입니다: 'routine_missed_demo'")), true);
assert.strictEqual(demo.isUnsupportedDemoScenario(new Error('MQTT 연결 실패')), false);
const live = demo.createLiveState();
assert.strictEqual(demo.liveTimeLabel(live, 1), '12:34:56');
assert.strictEqual(demo.liveTimeLabel(live, 330), '12:40:25');
assert.strictEqual(demo.liveViewEnd(live), 330);
assert.deepStrictEqual(Array.from(demo.liveTicks(330)).sort((a, b) => a - b), [1, 31, 61, 91, 121]);
for (const offset of [0, 30, 90, 150, 270, 329]) {
  assert.strictEqual(demo.liveTimeLabel(live, offset + 1), demo.formatTime(offset));
}
assert.strictEqual(demo.applyLiveSample(live, { house: 'H002', scenario: 'routine_missed_demo', sec: 1, totalP: 55 }), false);
assert.strictEqual(demo.applyLiveSample(live, { house: 'H001', scenario: 'prolonged_use', sec: 1, totalP: 55 }), false);
assert.strictEqual(demo.applyLiveSample(live, { house: 'H001', scenario: 'routine_missed_demo', sec: 331, totalP: 55 }), false);

const pad = n => String(n).padStart(2, '0');
const sample = (sec, status = 'running') => {
  const t = 12 * 3600 + 34 * 60 + 55 + sec;
  return {
    house: 'H001', scenario: 'routine_missed_demo', sec, status,
    measurementAvailable: true, totalP: 55.5,
    simTimeKst: `${pad(Math.floor(t / 3600))}:${pad(Math.floor(t % 3600 / 60))}:${pad(t % 60)}`,
    simDateKst: '2026-09-24',
    devices: { microwave: { state: 'OFF', enabled: false } }
  };
};

assert.strictEqual(demo.applyLiveSample(live, sample(1)), true);
assert.strictEqual(demo.applyLiveSample(live, sample(3)), true);
assert.strictEqual(live.lastSec, 3);
assert.strictEqual(live.received, 2);
assert.strictEqual(live.power[2], null);
assert.strictEqual(live.simTime, '12:34:58');
assert.strictEqual(live.onSec, null);
assert.strictEqual(demo.liveUsageSeconds(live), 0);
assert.strictEqual(demo.liveStatusText(live), '전자레인지 미사용 · 대기전력 발행 중 (3/330)');
demo.applyLiveSample(live, sample(3));
assert.strictEqual(live.received, 2);

demo.applyLiveSample(live, sample(299));
assert.strictEqual(live.onSec, null);
assert.strictEqual(demo.liveStatusText(live), '전자레인지 미사용 · 대기전력 발행 중 (299/330)');
demo.applyLiveSample(live, sample(330, 'completed'));
assert.strictEqual(live.status, 'completed');
assert.strictEqual(live.onSec, null);
assert.strictEqual(demo.liveStatusText(live), 'MQTT 발행 330건 완료 · 전자레인지 미사용');

const fallback = demo.createLiveState('manual');
assert.strictEqual(demo.applyLiveSample(fallback, sample(1)), false);
assert.strictEqual(demo.applyLiveSample(fallback, { ...sample(1), scenario: 'manual' }), true);
assert.strictEqual(demo.liveTimeLabel(fallback, 330), '12:40:25');

console.log('routine demo UI tests passed');
