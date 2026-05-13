import { HTTP_STATUS } from "../config/constants.ts";

type StreamReadinessLogger = {
  debug?: (tag: string, message: string) => void;
  warn?: (tag: string, message: string) => void;
};

export type StreamReadinessResult =
  | { ok: true; response: Response }
  | { ok: false; response: Response; reason: string; code: string; type: string };

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function hasNonEmptyString(value: unknown): boolean {
  return typeof value === "string" && value.length > 0;
}

function hasUsefulValue(value: unknown): boolean {
  if (hasNonEmptyString(value)) return true;
  if (Array.isArray(value)) return value.some(hasUsefulValue);
  if (!isRecord(value)) return false;

  for (const key of [
    "content",
    "text",
    "delta",
    "reasoning_content",
    "reasoning",
    "partial_json",
    "arguments",
    "name",
    "thought",
    "error",
    "executableCode",
    "codeExecutionResult",
    "finish_reason",  // Fix A: OpenAI finish_reason signals a completed stream
    "stop_reason",   // Fix B: Anthropic stop_reason signals a completed stream
  ]) {
    const candidate = value[key];
    if (hasNonEmptyString(candidate)) return true;
    if ((Array.isArray(candidate) || isRecord(candidate)) && hasUsefulValue(candidate)) return true;
  }

  for (const key of [
    "tool_calls",
    "tool_use",
    "function",
    "functionCall",
    "function_call",
    "function_call_output",
    "output",
    "content_block",
    "response",
    "choices",
    "candidates",
    "parts",
    "message",  // Fix B: Anthropic message_start wraps the message object
  ]) {
    if (hasUsefulValue(value[key])) return true;
  }

  return false;
}

function hasUsefulJsonPayload(payload: unknown): boolean {
  if (!isRecord(payload)) return false;
  return hasUsefulValue(payload);
}

export function hasUsefulStreamContent(text: string): boolean {
  const lines = text.split(/\r?\n/);

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith(":")) continue;
    if (/^event:\s*(?:ping|keepalive)$/i.test(trimmed)) continue;
    if (!trimmed.startsWith("data:")) continue;

    const data = trimmed.slice(5).trim();
    if (!data || data === "[DONE]") continue;

    try {
      if (hasUsefulJsonPayload(JSON.parse(data))) return true;
    } catch {
      if (data.length > 0) return true;
    }
  }

  return false;
}

function createErrorResponse(
  status: number,
  message: string,
  code: string,
  type: string
): Response {
  return new Response(
    JSON.stringify({
      error: {
        message,
        type,
        code,
      },
    }),
    { status, headers: { "Content-Type": "application/json" } }
  );
}

function prependBufferedChunks(
  chunks: Uint8Array[],
  reader: ReadableStreamDefaultReader<Uint8Array>
): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        for (const chunk of chunks) {
          controller.enqueue(chunk);
        }

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value) controller.enqueue(value);
        }

        controller.close();
      } catch (error) {
        controller.error(error);
      } finally {
        reader.releaseLock();
      }
    },
    async cancel(reason) {
      await reader.cancel(reason).catch(() => {});
      reader.releaseLock();
    },
  });
}

function readWithTimeout(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  timeoutMs: number
): Promise<ReadableStreamReadResult<Uint8Array>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("STREAM_READINESS_TIMEOUT")), timeoutMs);
    reader.read().then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error) => {
        clearTimeout(timeout);
        reject(error);
      }
    );
  });
}

export async function ensureStreamReadiness(
  response: Response,
  options: {
    timeoutMs: number;
    provider?: string | null;
    model?: string | null;
    log?: StreamReadinessLogger | null;
  }
): Promise<StreamReadinessResult> {
  if (!response.body || options.timeoutMs <= 0) return { ok: true, response };

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  const decoder = new TextDecoder();
  let bufferedText = "";
  const startedAt = Date.now();
  const deadline = startedAt + options.timeoutMs;
  let handedOffReader = false;
  let sawAnyDataLine = false;

  function checkAnyDataLine(text: string): boolean {
    return text.split(/\r?\n/).some((line) => {
      const t = line.trim();
      return t.startsWith("data:") && t.slice(5).trim() !== "[DONE]" && t.slice(5).trim() !== "";
    });
  }

  try {
    while (true) {
      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        const reason = `Stream produced no useful content within ${options.timeoutMs}ms`;
        options.log?.warn?.(
          "STREAM",
          `${reason} (${options.provider || "provider"}/${options.model || "unknown"})`
        );
        await reader.cancel(reason).catch(() => {});
        return {
          ok: false,
          reason,
          code: "STREAM_READINESS_TIMEOUT",
          type: "stream_timeout",
          response: createErrorResponse(
            HTTP_STATUS.GATEWAY_TIMEOUT,
            reason,
            "STREAM_READINESS_TIMEOUT",
            "stream_timeout"
          ),
        };
      }

      let readResult: ReadableStreamReadResult<Uint8Array>;
      try {
        readResult = await readWithTimeout(reader, remainingMs);
      } catch {
        const reason = `Stream produced no useful content within ${options.timeoutMs}ms`;
        options.log?.warn?.(
          "STREAM",
          `${reason} (${options.provider || "provider"}/${options.model || "unknown"})`
        );
        await reader.cancel(reason).catch(() => {});
        return {
          ok: false,
          reason,
          code: "STREAM_READINESS_TIMEOUT",
          type: "stream_timeout",
          response: createErrorResponse(
            HTTP_STATUS.GATEWAY_TIMEOUT,
            reason,
            "STREAM_READINESS_TIMEOUT",
            "stream_timeout"
          ),
        };
      }

      if (readResult.done) {
        // Fix C: distinguish clean EOF (valid SSE frames sent, just no text content)
        // from true early EOF (connection dropped with zero data lines).
        if (sawAnyDataLine) {
          options.log?.debug?.(
            "STREAM",
            `Stream completed with valid SSE but no text content (${options.provider || "provider"}/${options.model || "unknown"})`
          );
          handedOffReader = true;
          return {
            ok: true,
            response: new Response(prependBufferedChunks(chunks, reader), {
              status: response.status,
              statusText: response.statusText,
              headers: response.headers,
            }),
          };
        }
        const reason = "Stream ended before producing useful content";
        options.log?.warn?.(
          "STREAM",
          `${reason} (${options.provider || "provider"}/${options.model || "unknown"})`
        );
        return {
          ok: false,
          reason,
          code: "STREAM_EARLY_EOF",
          type: "stream_early_eof",
          response: createErrorResponse(
            HTTP_STATUS.BAD_GATEWAY,
            reason,
            "STREAM_EARLY_EOF",
            "stream_early_eof"
          ),
        };
      }

      if (!readResult.value) continue;
      chunks.push(readResult.value);
      bufferedText += decoder.decode(readResult.value, { stream: true });

      if (!sawAnyDataLine) {
        sawAnyDataLine = checkAnyDataLine(bufferedText);
      }

      if (hasUsefulStreamContent(bufferedText)) {
        options.log?.debug?.(
          "STREAM",
          `Stream readiness confirmed in ${Date.now() - startedAt}ms (${options.provider || "provider"}/${options.model || "unknown"})`
        );
        handedOffReader = true;
        return {
          ok: true,
          response: new Response(prependBufferedChunks(chunks, reader), {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
          }),
        };
      }
    }
  } finally {
    if (!handedOffReader) {
      reader.releaseLock();
    }
  }
}
