import { authApi } from "./client";

export function parseEvent(block: string): { event: string; data: string } | null {
  let event = "message";
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
  }
  return data.length ? { event, data: data.join("\n") } : null;
}

export async function consumeEventStream(
  stream: ReadableStream<Uint8Array>,
  signal: AbortSignal,
  onEvent: (event: string, data: unknown) => void,
) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (!signal.aborted) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer = `${buffer}${decoder.decode(value, { stream: true })}`.replace(/\r\n/g, "\n");
      let boundary = buffer.indexOf("\n\n");
      while (boundary >= 0) {
        const parsed = parseEvent(buffer.slice(0, boundary));
        buffer = buffer.slice(boundary + 2);
        if (parsed) onEvent(parsed.event, JSON.parse(parsed.data) as unknown);
        boundary = buffer.indexOf("\n\n");
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

export async function readSubjectStream(
  signal: AbortSignal,
  onOpen: () => void,
  onEvent: (event: string, data: unknown) => void,
) {
  const response = await authApi.get<ReadableStream<Uint8Array>>("/monitoring/stream", {
    adapter: "fetch",
    responseType: "stream",
    signal,
    headers: { Accept: "text/event-stream" },
  });
  if (
    !String(response.headers["content-type"] ?? "").includes("text/event-stream") ||
    !response.data?.getReader
  )
    throw new Error("실시간 응답을 확인할 수 없습니다.");
  onOpen();
  await consumeEventStream(response.data, signal, onEvent);
}
