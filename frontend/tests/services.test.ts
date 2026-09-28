import MockAdapter from 'axios-mock-adapter'
import { afterEach, describe, expect, it } from 'vitest'
import { login } from '../src/api/auth'
import { authApi, publicApi } from '../src/api/client'
import { answerNotification, getMyDashboard, getPowerUsage, getSubjectEvents, getSubjects, registerPushSubscription, registerSubject, updateAwayMode } from '../src/api/monitoring'

const authMock = new MockAdapter(authApi)
const publicMock = new MockAdapter(publicApi)

afterEach(() => {
  authMock.reset()
  publicMock.reset()
})

describe('도메인 API 서비스', () => {
  it('로그인 응답 토큰을 반환한다', async () => {
    const tokens = { accessToken: 'access', refreshToken: 'refresh', expiresIn: 60 }
    publicMock.onPost('/auth/login', { email: 'user@test.com', password: 'password' }).reply(200, tokens)
    await expect(login('user@test.com', 'password')).resolves.toEqual(tokens)
  })

  it('대시보드 조회 API를 호출한다', async () => {
    authMock.onGet('/monitoring/dashboard').reply(200, { subjects: [] })
    authMock.onGet('/monitoring/my-dashboard').reply(200, { subjectId: '1', name: '사용자' })
    await expect(getSubjects()).resolves.toEqual({ subjects: [] })
    await expect(getMyDashboard()).resolves.toMatchObject({ subjectId: '1' })
  })

  it('대상자 등록과 외출 설정을 전송한다', async () => {
    const subject = { name: '홍길동', birthDate: '1950-01-01', phone: '01012345678', householdId: 'A1', address: '서울', addressDetail: '', managerMemo: '' }
    authMock.onPost('/monitoring/subjects', subject).reply(204)
    authMock.onPut('/monitoring/my-dashboard/away-mode', { enabled: false }).reply(204)
    await expect(registerSubject(subject)).resolves.toBeUndefined()
    await expect(updateAwayMode({ enabled: false })).resolves.toBeUndefined()
  })

  it('알림 응답과 상세 조회 파라미터를 전송한다', async () => {
    authMock.onPut('/monitoring/notifications/10/responses').reply((config) => {
      const body = JSON.parse(config.data as string) as { answer: string; source: string }
      return body.answer === 'yes' && body.source === 'user' ? [204] : [400]
    })
    authMock.onGet('/monitoring/subjects/1/events', { params: { size: 20, cursor: 'next' } }).reply(200, { events: [], pagination: { hasNext: false, nextCursor: null } })
    authMock.onGet('/monitoring/subjects/1/power-usage', { params: { date: '2026-09-18' } }).reply(200, { date: '2026-09-18', hourlyUsage: [] })
    await expect(answerNotification('10', 'yes')).resolves.toBeUndefined()
    await expect(getSubjectEvents('1', 'next')).resolves.toMatchObject({ events: [] })
    await expect(getPowerUsage('1', '2026-09-18')).resolves.toMatchObject({ date: '2026-09-18' })
  })

  it('Web Push 구독 정보를 전송한다', async () => {
    const subscription = {
      endpoint: 'https://push.example.com/subscription',
      expirationTime: null,
      keys: { p256dh: 'public-key', auth: 'auth-secret' },
    }
    authMock.onPost('/monitoring/push-subscriptions', subscription).reply(204)
    await expect(registerPushSubscription(subscription)).resolves.toBeUndefined()
  })

  it('알림 이력의 조회 기간과 다음 페이지 커서를 함께 전송한다', async () => {
    const range = { from: '2026-06-29', to: '2026-09-26' }
    authMock.onGet('/monitoring/subjects/1/events', { params: { size: 20, cursor: 'next', ...range } })
      .reply(200, { events: [], pagination: { hasNext: false, nextCursor: null } })
    await expect(getSubjectEvents('1', 'next', range)).resolves.toMatchObject({ events: [] })
  })
})
