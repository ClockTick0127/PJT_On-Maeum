import { describe, expect, it } from 'vitest'
import { responseLabel } from '../src/types/monitoring'

describe('responseLabel', () => {
  it('응답 상태를 사용자 문구로 변환한다', () => {
    expect(responseLabel()).toBe('알림 없음')
    expect(responseLabel({ status: 'ANSWERED', answer: 'yes', respondedAt: null })).toBe('도움 요청')
    expect(responseLabel({ status: 'ANSWERED', answer: 'no', respondedAt: null })).toBe('안전 응답')
    expect(responseLabel({ status: 'PENDING', answer: null, respondedAt: null })).toBe('응답 대기')
    expect(responseLabel({ status: 'CUSTOM', answer: null, respondedAt: null })).toBe('CUSTOM')
  })
})
