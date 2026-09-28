import { describe, expect, it } from 'vitest'
import { clearSession, getAccessToken, getRefreshToken, hasSession, saveSession, SESSION_EVENT } from '../src/api/tokenStorage'

describe('tokenStorage', () => {
  it('토큰 저장과 세션 변경 이벤트를 함께 처리한다', () => {
    let notified = false
    window.addEventListener(SESSION_EVENT, () => { notified = true }, { once: true })
    saveSession({ accessToken: 'access', refreshToken: 'refresh', expiresIn: 60 })
    expect(getAccessToken()).toBe('access')
    expect(getRefreshToken()).toBe('refresh')
    expect(hasSession()).toBe(true)
    expect(notified).toBe(true)
  })

  it('세션 토큰을 모두 제거한다', () => {
    saveSession({ accessToken: 'access', refreshToken: 'refresh', expiresIn: 60 })
    clearSession()
    expect(getAccessToken()).toBeNull()
    expect(getRefreshToken()).toBeNull()
    expect(hasSession()).toBe(false)
  })

  it('로그인 유지 선택 시 브라우저를 닫아도 남는 저장소를 사용한다', () => {
    saveSession({ accessToken: 'access', refreshToken: 'refresh', expiresIn: 60 }, true)
    expect(localStorage.getItem('onmaeum.accessToken')).toBe('access')
    expect(sessionStorage.getItem('onmaeum.accessToken')).toBeNull()
  })
})
