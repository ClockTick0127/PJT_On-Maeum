import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, expect, it, vi } from 'vitest'
import SceneDemoPage from '../src/pages/SceneDemoPage'

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
const scene = { appliance: 'kettle', house: 'H046', family: 'TCN', householdId: 'house', runId: 'run', profileId: 'profile', firstIndex: 100, clipIndex: 100, lastIndex: 101, threshold: { on: .99, off: .99 }, stateParity: true, maxScoreDifference: .008 }
const snapshot = (index: number) => ({ schema_version: 2, household_id: 'house', run_id: 'run', profile_id: 'profile', source_index: index, observed_at: '2026-09-21T12:00:00+09:00', ready: true, target_appliance: 'KETTLE', measurement: { active_power: 1500 }, runtime: { checkpoint_sha256: 'abc', device: 'cpu', dtype: 'float32' }, appliances: [ { appliance_type: 'KETTLE', state: 'ON', probability: .995, inferred: true }, { appliance_type: 'IRON', state: 'UNKNOWN', probability: null, inferred: false } ] })
let root: ReturnType<typeof createRoot> | undefined
afterEach(async () => { if (root) await act(async () => root?.unmount()); document.body.innerHTML = ''; vi.unstubAllGlobals() })
async function render() {
  const node = document.createElement('div'); document.body.append(node); root = createRoot(node)
  await act(async () => { root?.render(<SceneDemoPage />); await new Promise(resolve => setTimeout(resolve, 20)) })
  return node
}
it('renders stored actual states, retains UNKNOWN and fetches exact source indices', async () => {
  const fetcher = vi.fn(async (url: string) => ({ ok: true, json: async () => url.startsWith('/scene-demo') ? [scene] : snapshot(Number(new URL(url, 'http://local').searchParams.get('sourceIndex'))) }))
  vi.stubGlobal('fetch', fetcher)
  const node = await render()
  expect(node.textContent).toContain('사용 중')
  expect(node.textContent).toContain('UNKNOWN')
  expect(node.textContent).toContain('이번 장면 미분석')
  expect(fetcher.mock.calls.map(c => c[0]).filter(u => u.includes('sourceIndex='))).toHaveLength(2)
})
it('shows backend failure without fabricated values', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url: string) => url.startsWith('/scene-demo') ? { ok: true, json: async () => [scene] } : { ok: false, status: 503 }))
  const node = await render()
  expect(node.querySelector('[role=alert]')?.textContent).toContain('503')
  expect(node.querySelector('.scene-metrics')).toBeNull()
})
it('rejects a response belonging to a different run', async () => {
  vi.stubGlobal('fetch', vi.fn(async (url: string) => ({ ok: true, json: async () => url.startsWith('/scene-demo') ? [scene] : { ...snapshot(100), run_id: 'other' } })))
  const node = await render()
  expect(node.querySelector('[role=alert]')?.textContent).toContain('일치하지 않습니다')
  expect(node.querySelector('.scene-metrics')).toBeNull()
})
