import { describe, expect, it } from 'vitest'
import { formatDateTime, formatShortDateTime, riskBadgeClass, telephoneHref, todayInSeoul } from '../src/utils/format'

describe('format 유틸리티', () => {
  it('잘못된 날짜는 기록 없음으로 표시한다', () => {
    expect(formatDateTime()).toBe('기록 없음')
    expect(formatDateTime('invalid')).toBe('기록 없음')
  })

  it('위험 단계별 Tailwind 클래스를 반환한다', () => {
    expect(riskBadgeClass('DANGER')).toContain('red')
    expect(riskBadgeClass('WARNING')).toContain('amber')
    expect(riskBadgeClass('NORMAL')).toContain('emerald')
  })

  it('전화 링크에서 표시용 문자를 제거한다', () => {
    expect(telephoneHref('010-1234-5678')).toBe('tel:01012345678')
  })

  it('짧은 날짜와 서울 기준 오늘을 표시한다', () => {
    expect(formatShortDateTime()).toBe('종료 시간 미정')
    expect(formatShortDateTime('2026-09-18T12:00:00+09:00')).toContain('9월')
    expect(todayInSeoul()).toMatch(/^\d{4}-\d{2}-\d{2}$/)
  })
})
