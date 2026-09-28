const assert = require('assert');
const fs = require('fs');
const path = require('path');

module.exports = async function testPowerflowUI({ getGlobal, setGlobal, getOrCreateElement, sandbox, htmlContent }) {
  const flow = name => getGlobal(name);
  const panel = getOrCreateElement('powerflowPanel');
  const lane = getOrCreateElement('powerflowMqttLane');
  const ring = getOrCreateElement('powerflowBurstRing');
  const gauge = getOrCreateElement('powerflowBurst');
  const css = fs.readFileSync(path.join(__dirname, '..', 'assets', 'waveform.css'), 'utf8');
  getGlobal('if (e2ePollTimer) { clearInterval(e2ePollTimer); e2ePollTimer = null; } e2eCurrentRunId = null;');
  await new Promise(resolve => setTimeout(resolve, 0));

  console.log('\n[Test 46] 분전반→MQTT 단일 경로와 발행 확인 경계');
  assert.ok(htmlContent.includes('id="powerflowPanel"'));
  assert.ok(!htmlContent.includes('id="powerflowAnalysis"'), '확인되지 않은 분석 서버는 이 패널에 그리지 않는다');
  assert.ok(htmlContent.includes('class="flow-lane-origin"'));
  assert.ok(htmlContent.includes('class="flow-lane-arrow"'));
  assert.ok(htmlContent.includes('class="flow-lane-destination"'));
  assert.ok(css.includes('.flow-packet::before') && css.includes('.flow-packet::after'),
    '계측 점의 잔상은 별도 데이터 이벤트 없이 장식으로만 표시한다');
  assert.ok(css.includes('.flow-panel[data-mode="live"] .flow-burst'));
  assert.ok(css.includes('.flow-panel[data-mode="burst"] .flow-stage'));
  assert.ok(!htmlContent.includes('flowPanelValue'), '전송 패널의 주 상태는 숫자에 의존하지 않아야 한다');
  flow('powerflowReset')();
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'idle');
  assert.strictEqual(lane.children.length, 0);
  flow('powerflowBeginRun')(null);
  flow('powerflowSetObservedHouse')('H001');
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'waiting');
  assert.strictEqual(panel.getAttribute('data-confirmed'), '0');
  assert.strictEqual(lane.children.length, 0, '실행 시작만으로 데이터가 움직이면 안 된다');

  const metric = { house: 'H001', status: 'running', sec: 1, totalP: 56.1,
    measurementAvailable: true, sensorFault: false, activeNames: [] };
  // 실제 화면 경로: SSE 처리 함수에서 흐름 패널에 이벤트를 전달한다.
  setGlobal('isSimulationRunning', true);
  getGlobal('activeConfiguredHouseholds = []');
  flow('processServerMetrics')(metric);
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'confirmed');
  assert.strictEqual(panel.getAttribute('data-confirmed'), '1');
  assert.strictEqual(lane.children.length, 1, '발행 완료 SSE에서만 점을 만든다');
  flow('powerflowApplyRealtime')({ ...metric, sec: 2 });
  assert.strictEqual(lane.children.length, 2, '연속 발행 확인은 각 점을 독립적으로 표시한다');
  await new Promise(resolve => setTimeout(resolve, 830));
  assert.strictEqual(getOrCreateElement('powerflowBroker').className, 'flow-device flow-broker flow-react',
    '확인된 점이 도달한 시점에 브로커가 짧게 반응한다');
  assert.ok(panel.getAttribute('aria-label').includes('MQTT 브로커 발행 확인까지 표시'));

  console.log('\n[Test 47] 결측·0건·일시정지·종료의 점 정리');
  flow('powerflowApplyRealtime')({ ...metric, sec: 2, totalP: null });
  assert.strictEqual(lane.children.length, 0);
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'waiting');
  flow('powerflowApplyRealtime')({ ...metric, sec: 3, measurementAvailable: false, totalP: null });
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'missing');
  assert.strictEqual(panel.getAttribute('data-fault'), '1');
  assert.strictEqual(lane.children.length, 0);
  flow('powerflowApplyRealtime')({ ...metric, sec: 4 });
  assert.strictEqual(lane.children.length, 1);
  flow('powerflowSetRunState')('paused');
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'paused');
  assert.strictEqual(lane.children.length, 0);
  flow('powerflowApplyRealtime')({ ...metric, sec: 5 });
  assert.strictEqual(lane.children.length, 0, '일시정지 중 늦은 SSE는 점을 만들지 않는다');
  flow('powerflowSetRunState')('running');
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'waiting');
  assert.strictEqual(lane.children.length, 0, '재개 자체는 발행 확인이 아니다');
  flow('powerflowApplyRealtime')({ ...metric, sec: 6 });
  flow('powerflowSetRunState')('completed');
  assert.strictEqual(lane.children.length, 0);
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'completed');

  console.log('\n[Test 48] 관찰 가구·새 실행 전환과 이전 반응 취소');
  flow('powerflowBeginRun')(null);
  flow('powerflowSetObservedHouse')('H001');
  flow('powerflowApplyRealtime')({ ...metric, sec: 7 });
  assert.strictEqual(lane.children.length, 1);
  flow('powerflowSetObservedHouse')('H002');
  assert.strictEqual(lane.children.length, 0);
  assert.strictEqual(panel.getAttribute('data-confirmed'), '0');
  flow('powerflowApplyRealtime')({ ...metric, sec: 8 });
  assert.strictEqual(lane.children.length, 0, '이전 가구의 SSE는 무시한다');
  await new Promise(resolve => setTimeout(resolve, 830));
  assert.strictEqual(getOrCreateElement('powerflowBroker').className, 'flow-device flow-broker',
    '전환 전에 예약된 브로커 반응은 실행되지 않는다');
  flow('powerflowBeginRun')(null);
  assert.strictEqual(panel.getAttribute('data-confirmed'), '0');
  flow('powerflowApplyRealtime')({ ...metric, house: 'H002', source: 'E2E', run_id: 'old-run' });
  assert.strictEqual(lane.children.length, 0, '새 레거시 실행은 지난 E2E SSE를 무시한다');

  console.log('\n[Test 49] BURST는 발행 건수만 링에 표시하며 0건은 정지');
  setGlobal('observedHouse', 'H002');
  const burst = (run_id, published_samples, planned_publish_samples, overall_status = 'RUNNING') => ({
    run_id, execution_mode: 'BURST', overall_status,
    households: [{ household_id: 'H002', published_samples, planned_publish_samples }]
  });
  flow('powerflowRenderE2E')(burst('burst-1', 0, 100));
  assert.strictEqual(panel.getAttribute('data-mode'), 'burst');
  assert.strictEqual(ring.style['--burst-progress'], '0%');
  assert.strictEqual(lane.children.length, 0);
  assert.strictEqual(panel.getAttribute('data-confirmed'), '0');
  flow('powerflowRenderE2E')(burst('burst-1', 25, 100));
  assert.strictEqual(ring.style['--burst-progress'], '25%');
  assert.strictEqual(gauge.getAttribute('aria-label'), 'MQTT 발행 25건 / 계획 100건');
  assert.strictEqual(lane.children.length, 0);
  flow('powerflowRenderE2E')(burst('burst-2', 0, 100));
  assert.strictEqual(ring.style['--burst-progress'], '0%', '새 실행에는 이전 링 진행이 남지 않는다');
  flow('powerflowRenderE2E')(burst('burst-2', 0, 100, 'PAUSED'));
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'paused');
  flow('powerflowRenderE2E')({ ...burst('burst-2', 0, 100),
    households: [{ household_id: 'H002', state: 'PAUSED', published_samples: 0, planned_publish_samples: 100 }] });
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'paused',
    '관찰 가구가 멈추면 전체 실행 중이어도 패널은 정지한다');

  console.log('\n[Test 50] E2E 실행 식별과 모션 최소화');
  flow('powerflowRenderE2E')({ run_id: 'live-1', execution_mode: 'ACCELERATED', overall_status: 'RUNNING' });
  flow('powerflowSetObservedHouse')('H002');
  flow('powerflowApplyRealtime')({ ...metric, house: 'H002', source: 'E2E', run_id: 'old-run' });
  assert.strictEqual(lane.children.length, 0, '이전 실행의 SSE는 무시한다');
  sandbox.matchMedia = () => ({ matches: true });
  flow('powerflowApplyRealtime')({ ...metric, house: 'H002', source: 'E2E', run_id: 'live-1' });
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'confirmed');
  assert.strictEqual(lane.children.length, 0, '모션 최소화 시 점 이동을 만들지 않는다');
  assert.ok(css.includes('@media (prefers-reduced-motion: reduce)'));
  sandbox.matchMedia = undefined;

  console.log('\n[Test 51] 발행량 요약은 상태 판정을 바꾸지 않고 수치만 더한다');
  const totalCount = getOrCreateElement('powerflowTotalCount');
  const rateText = getOrCreateElement('powerflowRate');
  const burstCount = getOrCreateElement('powerflowBurstCount');
  const burstPercent = getOrCreateElement('powerflowBurstPercent');
  flow('powerflowReset')();
  assert.strictEqual(totalCount.textContent, '0');
  assert.strictEqual(rateText.textContent, '0.0');
  assert.strictEqual(burstCount.textContent, '0건 / 0건');
  assert.strictEqual(burstPercent.textContent, '0.0%');
  flow('powerflowBeginRun')(null);
  flow('powerflowSetObservedHouse')('H001');
  flow('powerflowApplyRealtime')({ ...metric, sec: 1 });
  flow('powerflowApplyRealtime')({ ...metric, sec: 2 });
  assert.strictEqual(totalCount.textContent, '2', '발행 확인된 샘플만 누적한다');
  flow('powerflowApplyRealtime')({ ...metric, sec: 3, measurementAvailable: false, totalP: null });
  assert.strictEqual(totalCount.textContent, '2', '결측은 누적에 들어가지 않는다');
  assert.strictEqual(panel.getAttribute('data-visual-state'), 'missing',
    '수치가 붙어도 상태 판정은 data-visual-state가 단독으로 한다');
  flow('powerflowSetObservedHouse')('H002');
  assert.strictEqual(totalCount.textContent, '0', '관찰 가구를 바꾸면 집계를 새로 센다');
  setGlobal('observedHouse', 'H002');
  flow('powerflowRenderE2E')(burst('burst-3', 25, 200));
  assert.strictEqual(burstCount.textContent, '25건 / 200건', 'BURST 링 옆에 실제 건수를 적는다');
  assert.strictEqual(burstPercent.textContent, '12.5%');
  assert.strictEqual(gauge.getAttribute('aria-label'), 'MQTT 발행 25건 / 계획 200건');
  assert.ok(htmlContent.includes('메인 분전반') && htmlContent.includes('MQTT 브로커'),
    '좌우 기기 아이콘에는 이름표가 붙어 있어야 한다');
  assert.ok(css.includes('content: "✓ 발행 확인"'), '상태 표식은 글리프와 문구를 함께 보여준다');
  assert.ok(htmlContent.includes('class="flow-lane-break"'), '결측 구간 라벨은 레인 안에 있어야 한다');
  assert.ok(css.includes('.flow-panel[data-visual-state="missing"] .flow-lane-break { display: inline-flex; }'),
    '결측 라벨은 missing 상태에서만 드러난다');
  assert.ok(css.includes('.flow-panel[data-visual-state="missing"] .flow-lane-arrow { display: none; }'),
    '결측일 때는 흐름 화살표를 지워 끊긴 경로로 보여준다');

  flow('powerflowReset')();
  console.log('✔ Test 46-51 통과: 발행 확인 범위, 상태별 정지, 링 진행, 실행 격리, 모션 최소화, 발행량 요약');
};
