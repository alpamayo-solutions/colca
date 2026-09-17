import { afterEach, describe, expect, it, vi } from "vitest";

// The browser build of mqtt.js, as a bundler resolves it: a default export and nothing else.
const { connect } = vi.hoisted(() => ({
  connect: vi.fn(() => ({
    on: vi.fn(),
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
    publish: vi.fn(),
    end: vi.fn(),
  })),
}));
// `connect: undefined` is what a module namespace without that export reads as; vitest would throw instead.
vi.mock("mqtt", () => ({ default: { connect }, connect: undefined }));

const { Live } = await import("../src/live.js");

afterEach(() => {
  vi.clearAllMocks();
});

describe("loading mqtt.js", () => {
  it("finds connect on the default export, as the browser build has it", async () => {
    const token = `e30.${Buffer.from(JSON.stringify({ sub: "till" })).toString("base64url")}.x`;
    const errors: Error[] = [];
    const live = new Live({ url: "wss://node:8885", token: () => token, onError: (e) => errors.push(e) });

    await vi.waitFor(() => {
      expect(connect).toHaveBeenCalledOnce();
    });
    expect(errors).toEqual([]);
    live.close();
  });
});
