import { FORMATS } from "../translator/formats.ts";

const DEFAULT_REASONING_HEADROOM_TOKENS = 4096;

function configuredMinimum(): number {
  const raw = Number(process.env.MIN_UPSTREAM_REASONING_TOKENS || "");
  if (Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  return DEFAULT_REASONING_HEADROOM_TOKENS;
}

function shouldReserveReasoningBudget(provider: string | null | undefined, targetFormat: string) {
  return targetFormat === FORMATS.OPENAI && provider?.startsWith("openai-compatible-") === true;
}

function raiseTokenField(
  body: Record<string, unknown>,
  field: "max_tokens" | "max_completion_tokens",
  minimum: number,
  log: { debug?: (tag: string, message: string) => void } | null | undefined,
  provider: string | null | undefined,
  model: string | null | undefined
) {
  const value = body[field];
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0 || value >= minimum) {
    return;
  }

  body[field] = minimum;
  log?.debug?.(
    "PARAMS",
    `Raised ${field} from ${value} to ${minimum} for ${provider}/${model} to avoid reasoning truncation`
  );
}

export function reserveReasoningTokenBudget(
  body: unknown,
  {
    provider,
    model,
    targetFormat,
    log,
  }: {
    provider?: string | null;
    model?: string | null;
    targetFormat: string;
    log?: { debug?: (tag: string, message: string) => void } | null;
  }
): unknown {
  if (!body || typeof body !== "object" || Array.isArray(body)) return body;
  if (!shouldReserveReasoningBudget(provider, targetFormat)) return body;

  const minimum = configuredMinimum();
  const record = body as Record<string, unknown>;
  raiseTokenField(record, "max_tokens", minimum, log, provider, model);
  raiseTokenField(record, "max_completion_tokens", minimum, log, provider, model);
  return body;
}
