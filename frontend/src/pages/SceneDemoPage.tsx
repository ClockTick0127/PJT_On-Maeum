import { useEffect, useRef, useState } from 'react'
import './SceneDemoPage.css'

type Scene = {
  appliance: string; house: string; family: string; householdId: string; runId: string
  profileId: string; firstIndex: number; clipIndex: number; lastIndex: number
  threshold: { on: number; off: number }; stateParity: boolean; maxScoreDifference: number
}
type Appliance = { appliance_type: string; state: 'ON' | 'OFF' | 'UNKNOWN'; probability: number | null; inferred: boolean }
type Snapshot = {
  source_index: number; observed_at: string; ready: boolean; target_appliance: string
  household_id: string; run_id: string; profile_id: string; schema_version: number
  measurement: { active_power: number }; appliances: Appliance[]
  runtime: { checkpoint_sha256: string; device: string; dtype: string }
}
const names: Record<string, string> = { kettle: '전기포트', induction: '인덕션', iron: '다리미', microwave: '전자레인지', hair_dryer: '헤어드라이어', vacuum_cleaner: '청소기' }
const icons: Record<string, string> = { kettle: '◡', induction: '◎', iron: '◭', microwave: '▣', hair_dryer: '↝', vacuum_cleaner: '⌁' }

export default function SceneDemoPage() {
  const [catalog, setCatalog] = useState<Scene[]>([])
  const [selected, setSelected] = useState(0)
  const [rows, setRows] = useState<Snapshot[]>([])
  const [position, setPosition] = useState(0)
  const [playing, setPlaying] = useState(false)
  const [speed, setSpeed] = useState(10)
  const [loaded, setLoaded] = useState(0)
  const [error, setError] = useState('')
  const [reload, setReload] = useState(0)
  const cache = useRef(new Map<string, Snapshot[]>())
  const scene = catalog[selected]
  const ended = rows.length > 0 && position === rows.length - 1
  useEffect(() => {
    const controller = new AbortController()
    fetch('/scene-demo-catalog.json', { signal: controller.signal, cache: 'no-store' })
      .then(r => { if (!r.ok) throw new Error('장면 목록을 불러오지 못했습니다.'); return r.json() })
      .then((data: Scene[]) => { if (!Array.isArray(data) || !data.length) throw new Error('먼저 데모 추론을 실행해 주세요.'); setCatalog(data) })
      .catch(e => { if (!controller.signal.aborted) setError(String(e.message)) })
    return () => controller.abort()
  }, [reload])
  useEffect(() => {
    if (!scene) return
    const controller = new AbortController()
    async function load() {
      setRows([]); setPosition(0); setPlaying(false); setError(''); setLoaded(0)
      const cached = cache.current.get(scene.runId)
      if (cached) { setRows(cached); setLoaded(cached.length); setPosition(scene.clipIndex - scene.firstIndex); return }
      const results: Snapshot[] = []
      for (let start = scene.firstIndex; start <= scene.lastIndex; start += 8) {
        const batch = await Promise.all(Array.from({ length: Math.min(8, scene.lastIndex - start + 1) }, async (_, i) => {
          const sourceIndex = start + i
          const query = new URLSearchParams({ householdId: scene.householdId, runId: scene.runId, profileId: scene.profileId, sourceIndex: String(sourceIndex) })
          const response = await fetch(`/api/monitoring/admin/selected-scene?${query}`, { signal: controller.signal, cache: 'no-store' })
          if (!response.ok) throw new Error(`백엔드 조회 실패 (${response.status}). Docker 데모 스택과 실행 결과를 확인해 주세요.`)
          const row: Snapshot = await response.json()
          if (row.schema_version !== 2 || row.run_id !== scene.runId || row.profile_id !== scene.profileId || row.source_index !== sourceIndex || row.household_id !== scene.householdId) throw new Error('선택한 장면과 백엔드 응답이 일치하지 않습니다.')
          return row
        }))
        results.push(...batch)
        if (controller.signal.aborted) return
        setLoaded(results.length)
      }
      if (!controller.signal.aborted) { cache.current.set(scene.runId, results); setRows(results); setPosition(scene.clipIndex - scene.firstIndex) }
    }
    void load().catch(e => { if (!controller.signal.aborted) setError(String(e.message)) })
    return () => controller.abort()
  }, [scene, reload])
  useEffect(() => {
    if (!playing || ended) return
    const timer = window.setInterval(() => setPosition(p => Math.min(p + 1, rows.length - 1)), 1000 / speed)
    return () => window.clearInterval(timer)
  }, [playing, speed, rows.length, ended])
  const row = rows[position]
  const target = row?.appliances.find(a => a.appliance_type === row.target_appliance)
  const plotRows = scene ? rows.filter(r => r.source_index >= scene.clipIndex) : []
  const maxPower = Math.max(1, ...plotRows.map(r => r.measurement.active_power))
  const powerLine = plotRows.map((r, i) => `${i / Math.max(1, plotRows.length - 1) * 1000},${180 - r.measurement.active_power / maxPower * 160}`).join(' ')
  const scoreLine = plotRows.map((r, i) => { const a = r.appliances.find(a => a.appliance_type === r.target_appliance); return a?.probability == null ? null : `${i / Math.max(1, plotRows.length - 1) * 1000},${180 - a.probability * 160}` }).filter(Boolean).join(' ')
  const cursor = scene && row ? Math.max(0, (row.source_index - scene.clipIndex) / (scene.lastIndex - scene.clipIndex) * 1000) : 0
  return <main className="scene-demo">
    <header className="scene-header"><a href="/" className="scene-brand"><span>n.</span> NILM LAB</a><span className="scene-pill">LOCAL DEMONSTRATION</span><span className="scene-connection">{rows.length ? '● 백엔드 조회 완료' : '○ 백엔드 연결 확인'}</span></header>
    <section className="scene-intro"><div><p className="scene-eyebrow">실제 모델 · 실제 데이터</p><h1>전력 속 가전의 움직임</h1><p>원본 전력에서 추론하고, 백엔드에 저장한 결과를 시간에 따라 살펴보세요.</p></div><div className="scene-mode">저장된 추론 결과 재생<small>MQTT → Kafka → 모델 → DB → API</small></div></section>
    <nav className="scene-tabs" aria-label="가전 장면 선택">{catalog.map((s, i) => <button key={s.profileId} aria-pressed={selected === i} onClick={() => setSelected(i)}><span>{icons[s.appliance]}</span>{names[s.appliance]}<small>{s.house} · {s.family}</small></button>)}</nav>
    <p className="scene-scope">각 탭은 개별 가정에서 선정한 한 가전의 장면입니다. 같은 가정의 가전 6종 동시 분석 결과가 아닙니다.</p>
    {error && <div role="alert" className="scene-error">{error} <button onClick={() => { cache.current.clear(); setReload(r => r + 1) }}>다시 연결</button></div>}
    {!rows.length && !error && <div role="status" className="scene-loading">실제 백엔드 결과를 불러오는 중 · {loaded} / {scene ? scene.lastIndex - scene.firstIndex + 1 : '—'}초</div>}
    {row && scene && <>
      <section className="scene-metrics"><article><label>선택 가전</label><strong>{names[scene.appliance]}</strong><small>{scene.house}의 선정 구간</small></article><article><label>분석 상태</label><strong className={`state-${target?.state}`}>{target?.state === 'ON' ? '사용 중' : target?.state === 'OFF' ? '사용 종료 / 대기' : '분석 준비 중'}</strong><small>{target?.state} · {row.ready ? '255초 입력 확보' : '입력 창 준비 중'}</small></article><article><label>가전 ON 확률</label><strong>{target?.probability == null ? '—' : `${(target.probability * 100).toFixed(1)}%`}</strong><small>ON ≥ {scene.threshold.on} / OFF {scene.threshold.on === scene.threshold.off ? '<' : '≤'} {scene.threshold.off}</small></article><article><label>가구 전체 유효전력</label><strong>{row.measurement.active_power.toFixed(0)} <em>W</em></strong><small>가전별 소비전력 추정값이 아닙니다</small></article></section>
      <section className="scene-chart"><div className="scene-chart-title"><div><h2>전력과 추론의 시간 흐름</h2><p>선정 구간 {plotRows.length}초 · 원본 1초 간격</p></div><div className="scene-legend"><span>━ 전체 전력</span><span>━ ON 확률</span></div></div><div className="scene-axis"><span>{Math.ceil(maxPower).toLocaleString()} W</span><span>100%</span></div><svg viewBox="0 0 1000 200" role="img" aria-label="가구 전체 전력과 선택 가전 ON 확률 시계열"><line x1="0" y1="100" x2="1000" y2="100" stroke="#e9edf0" /><line x1="0" y1={180 - scene.threshold.on * 160} x2="1000" y2={180 - scene.threshold.on * 160} stroke="#d0d8cd" strokeDasharray="5 5"/><polyline points={powerLine} fill="none" stroke="#92a8ba" strokeWidth="2"/><polyline points={scoreLine} fill="none" stroke="#128468" strokeWidth="3"/><line x1={cursor} x2={cursor} y1="0" y2="190" stroke="#db8d32" strokeWidth="2"/></svg><div className="scene-axis"><span>선정 구간 시작</span><span>선정 구간 종료</span></div>
      <div className="scene-controls"><button className="scene-play" onClick={() => { if (ended) { setPosition(scene.clipIndex - scene.firstIndex); setPlaying(true) } else setPlaying(p => !p) }}>{playing && !ended ? 'Ⅱ 일시정지' : '▶ 재생'}</button><button onClick={() => { setPlaying(false); setPosition(scene.clipIndex - scene.firstIndex) }}>구간 시작</button><select aria-label="재생 속도" value={speed} onChange={e => setSpeed(Number(e.target.value))}><option value={1}>1×</option><option value={10}>10×</option><option value={30}>30×</option></select><span>{position < 254 ? `준비 ${position + 1}/255초` : `구간 +${position - 254}초`} / {plotRows.length - 1}초</span></div><input aria-label="재생 시점" type="range" min={0} max={rows.length - 1} value={position} onChange={e => { setPlaying(false); setPosition(Number(e.target.value)) }}/><div className="scene-time">시연 이벤트 시각 {row.observed_at} · {ended ? '재생 완료' : `원본 인덱스 ${row.source_index}`}</div></section>
      <section className="scene-appliances" aria-label="가전별 분석 상태">{row.appliances.map(a => <article key={a.appliance_type} className={a.inferred ? 'active' : ''}><span>{names[a.appliance_type.toLowerCase()]}</span><strong className={`state-${a.state}`}>{a.state}</strong><small>{a.appliance_type === row.target_appliance ? a.inferred ? '선택 모델 분석' : '입력 창 준비' : '이번 장면 미분석'}</small></article>)}</section>
      <footer className="scene-details"><p>선정 구간 한정 시연입니다. OFF 대조군·다른 날짜의 검수에서 오탐 및 미탐이 확인되어 일반 환경 성능을 보장하지 않습니다.</p><details><summary>실행 및 검수 정보</summary><p>{scene.runId} · {scene.profileId}</p><p>{row.runtime.device} / {row.runtime.dtype} · H200 기준 상태 {scene.stateParity ? '일치' : '차이 있음'} · 최대 확률 차이 {scene.maxScoreDifference.toFixed(6)}</p><code>체크포인트 SHA256 {row.runtime.checkpoint_sha256}</code><p>표시용 정답·예상 점수 주입 없음. 조회 실패 시 결과를 대체하지 않습니다.</p></details></footer>
    </>}
  </main>
}
