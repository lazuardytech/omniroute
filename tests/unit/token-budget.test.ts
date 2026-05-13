import test from "node:test";
import assert from "node:assert/strict";

const { reserveReasoningTokenBudget } = await import("../../open-sse/utils/tokenBudget.ts");

test("reserveReasoningTokenBudget raises small OpenAI-compatible max_tokens", () => {
  const body = { model: "zen", max_tokens: 100 };

  reserveReasoningTokenBudget(body, {
    provider: "openai-compatible-chat-test",
    model: "zen",
    targetFormat: "openai",
  });

  assert.equal(body.max_tokens, 4096);
});

test("reserveReasoningTokenBudget leaves non-compatible providers unchanged", () => {
  const body = { model: "claude-sonnet", max_tokens: 100 };

  reserveReasoningTokenBudget(body, {
    provider: "claude",
    model: "claude-sonnet",
    targetFormat: "claude",
  });

  assert.equal(body.max_tokens, 100);
});

test("reserveReasoningTokenBudget preserves sufficient max_completion_tokens", () => {
  const body = { model: "zen", max_completion_tokens: 8192 };

  reserveReasoningTokenBudget(body, {
    provider: "openai-compatible-chat-test",
    model: "zen",
    targetFormat: "openai",
  });

  assert.equal(body.max_completion_tokens, 8192);
});
