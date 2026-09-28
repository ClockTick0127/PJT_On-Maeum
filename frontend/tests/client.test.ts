import MockAdapter from 'axios-mock-adapter'
import { afterEach, describe, expect, it } from 'vitest'
import { authApi, getApiErrorMessage, publicApi, request } from '../src/api/client'
import { clearSession, getAccessToken, saveSession } from '../src/api/tokenStorage'

const authMock = new MockAdapter(authApi)
const publicMock = new MockAdapter(publicApi)
afterEach(() => {
  authMock.reset()
  publicMock.reset()
})

describe('axios 공통 클라이언트', () => {
  it('인증 요청에 액세스 토큰을 포함한다', async () => {
    saveSession({ accessToken: 'access', refreshToken: 'refresh', expiresIn: 60 })
    authMock.onGet('/monitoring/dashboard').reply((config) => [200, { authorization: config.headers?.Authorization }])
    await expect(request<{ authorization: string }>({ url: '/monitoring/dashboard' })).resolves.toEqual({ authorization: 'Bearer access' })
  })

  it('401 응답에서 토큰을 갱신하고 요청을 재시도한다', async () => {
    saveSession({ accessToken: 'old', refreshToken: 'refresh', expiresIn: 60 })
    authMock.onGet('/protected').replyOnce(401).onGet('/protected').reply(200, { ok: true })
    publicMock.onPost('/auth/refresh').reply(200, { accessToken: 'new', refreshToken: 'next', expiresIn: 60 })
    await expect(request<{ ok: boolean }>({ url: '/protected' })).resolves.toEqual({ ok: true })
    expect(getAccessToken()).toBe('new')
  })

  it('서버 오류 응답을 사용자 문구로 변환한다', async () => {
    authMock.onGet('/failed').reply(403)
    try {
      await request({ url: '/failed' })
    } catch (error) {
      expect(getApiErrorMessage(error)).toBe('이 화면에 접근할 권한이 없습니다.')
    }
  })

  it('로그아웃 중 끝난 갱신 요청이 세션을 복구하지 않는다', async () => {
    saveSession({ accessToken: 'old', refreshToken: 'refresh', expiresIn: 60 })
    authMock.onGet('/protected').reply(401)
    publicMock.onPost('/auth/refresh').reply(() => {
      clearSession()
      return [200, { accessToken: 'new', refreshToken: 'next', expiresIn: 60 }]
    })
    await expect(request({ url: '/protected' })).rejects.toThrow('로그인이 종료되었습니다.')
    expect(getAccessToken()).toBeNull()
  })
})
