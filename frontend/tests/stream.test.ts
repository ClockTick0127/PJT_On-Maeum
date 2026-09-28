import { describe, expect, it } from 'vitest'
import { consumeEventStream, parseEvent } from '../src/api/stream'

describe('parseEvent', () => {
  it('SSE 주석과 데이터 없는 이벤트를 무시한다', () => {
    expect(parseEvent(': heartbeat')).toBeNull()
  })

  it('이벤트 이름과 여러 줄 데이터를 조합한다', () => {
    expect(parseEvent(': comment\nevent: subject-status\ndata: {\ndata: "version": 2}')).toEqual({
      event: 'subject-status', data: '{\n"version": 2}',
    })
  })

  it('나뉜 CRLF 청크와 여러 이벤트를 순서대로 처리한다', async () => {
    const encoder = new TextEncoder()
    const chunks = [': connected\r', '\n\r', '\nevent: subject-status\r\ndata: {"version":2}\r', '\n\r\ndata: {"version":3}\n\n']
    const stream = new ReadableStream({ start(controller) {
      chunks.forEach((chunk) => controller.enqueue(encoder.encode(chunk)))
      controller.close()
    } })
    const received: [string, unknown][] = []
    await consumeEventStream(stream, new AbortController().signal, (event, data) => received.push([event, data]))
    expect(received).toEqual([
      ['subject-status', { version: 2 }],
      ['message', { version: 3 }],
    ])
  })
})
