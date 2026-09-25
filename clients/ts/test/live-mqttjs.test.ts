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

describe("renewing over mqtt.js", () => {
  it("sends the new token in an AUTH packet and takes the node's answer", async () => {
    vi.useFakeTimers();
    const handlers = new Map<string, ((...args: unknown[]) => void)[]>();
    const sent: Record<string, unknown>[] = [];
    const fake = {
      on: vi.fn((event: string, handler: (...args: unknown[]) => void) => {
        handlers.set(event, [...(handlers.get(event) ?? []), handler]);
      }),
      subscribe: vi.fn(),
      unsubscribe: vi.fn(),
      publish: vi.fn(),
      end: vi.fn(),
      _sendPacket: vi.fn((packet: Record<string, unknown>) => sent.push(packet)),
      handleAuth: vi.fn((_packet: unknown, callback: () => void) => {
        callback();
      }),
    };
    connect.mockReturnValue(fake);
    const claims = (n: number): string =>
      Buffer.from(JSON.stringify({ sub: "till", exp: Math.floor(Date.now() / 1000) + 300, n })).toString(
        "base64url",
      );
    let n = 0;
    const live = new Live({ url: "wss://node:8885", token: () => `e30.${claims(n++)}.x` });
    await vi.advanceTimersByTimeAsync(0);
    for (const handler of handlers.get("connect") ?? []) {
      handler({ properties: { authenticationMethod: "colca-token" } });
    }

    await vi.advanceTimersByTimeAsync(270_000);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      cmd: "auth",
      reasonCode: 0x19,
      properties: {
        authenticationMethod: "colca-token",
        authenticationData: expect.stringMatching(/^e30\./),
      },
    });

    // mqtt.js hands the node's AUTH answer to handleAuth; the renewal settles and the next one is scheduled.
    const accepted = (fake as unknown as { handleAuth: (p: unknown, cb: () => void) => void }).handleAuth;
    accepted({ reasonCode: 0 }, () => undefined);
    await vi.advanceTimersByTimeAsync(270_000);
    expect(sent).toHaveLength(2);
    expect(connect).toHaveBeenCalledOnce();
    live.close();
    vi.useRealTimers();
  });
});
