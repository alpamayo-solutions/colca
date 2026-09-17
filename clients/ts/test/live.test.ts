import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  CommandTimeout,
  Live,
  type LiveOptions,
  type LiveValue,
  type MqttConnectOptions,
} from "../src/live.js";

type Handler = (...args: unknown[]) => void;

/** Stands in for mqtt.js: records what it is asked and lets a test play the broker. */
class FakeClient {
  readonly subscribed: string[] = [];
  readonly subscribedQos: [string, number][] = [];
  readonly unsubscribed: string[] = [];
  readonly sent: { topic: string; body: Record<string, unknown>; qos: number }[] = [];
  /** Set to hold SUBACKs back until `grant()`. */
  holdSubacks = false;
  /** Set to make the node refuse publishes. */
  refusePublish: Error | undefined;
  ended = false;
  readonly #handlers = new Map<string, Handler[]>();
  readonly #held: (() => void)[] = [];

  constructor(readonly options: MqttConnectOptions) {}

  on(event: string, handler: Handler): this {
    this.#handlers.set(event, [...(this.#handlers.get(event) ?? []), handler]);
    return this;
  }

  subscribe(
    filters: string[],
    options: { qos: number },
    callback?: (error: Error | null, granted: { topic: string; qos: number }[]) => void,
  ): void {
    this.subscribed.push(...filters);
    for (const filter of filters) this.subscribedQos.push([filter, options.qos]);
    const answer = (): void =>
      callback?.(
        null,
        filters.map((topic) => ({ topic, qos: options.qos })),
      );
    if (this.holdSubacks) this.#held.push(answer);
    else answer();
  }

  grant(): void {
    for (const answer of this.#held.splice(0)) answer();
  }

  unsubscribe(filters: string[]): void {
    this.unsubscribed.push(...filters);
  }

  publish(
    topic: string,
    message: string,
    options: { qos: number },
    callback?: (error?: Error) => void,
  ): void {
    this.sent.push({ topic, body: JSON.parse(message) as Record<string, unknown>, qos: options.qos });
    callback?.(this.refusePublish);
  }

  end(): void {
    this.ended = true;
  }

  emit(event: string, ...args: unknown[]): void {
    for (const handler of this.#handlers.get(event) ?? []) handler(...args);
  }

  /** The broker delivering a message to this client. */
  deliver(topic: string, payload: unknown, retain = false): void {
    const bytes =
      payload === undefined ? new Uint8Array() : new TextEncoder().encode(JSON.stringify(payload));
    this.emit("message", topic, bytes, { retain });
  }
}

function jwt(claims: Record<string, unknown>): string {
  const part = (value: unknown): string => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${part({ alg: "none" })}.${part(claims)}.signature`;
}

const T = "steine/v1/_Metric/n-technikum/wisewoods/line1/mas2/grit";

function setup(options: Partial<LiveOptions> = {}): {
  live: Live;
  clients: FakeClient[];
  tokens: string[];
  errors: Error[];
} {
  const clients: FakeClient[] = [];
  const tokens: string[] = [];
  const errors: Error[] = [];
  const live = new Live({
    url: "wss://node:8885",
    token: () => {
      const token = jwt({ sub: "till", exp: Math.floor(Date.now() / 1000) + 300, n: tokens.length });
      tokens.push(token);
      return token;
    },
    connect: (_url, connectOptions) => {
      const client = new FakeClient(connectOptions);
      clients.push(client);
      return client as never;
    },
    onError: (error) => errors.push(error),
    ...options,
  });
  return { live, clients, tokens, errors };
}

/** Let the token and connect promises settle. */
const settle = (): Promise<void> => vi.advanceTimersByTimeAsync(0).then(() => undefined);

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-09-17T08:00:00Z"));
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("connecting", () => {
  it("presents the token with its sub as the username, and lets nothing reconnect behind its back", async () => {
    const { clients, tokens } = setup();
    await settle();

    expect(clients).toHaveLength(1);
    expect(clients[0].options.username).toBe("till");
    expect(clients[0].options.password).toBe(tokens[0]);
    expect(clients[0].options.reconnectPeriod).toBe(0);
    expect(clients[0].options.clean).toBe(true);
  });

  it("sends what was subscribed before the connection was up", async () => {
    const { live, clients } = setup();
    live.subscribe(T, () => undefined);
    live.subscribe("steine/v1/_Metric/n-technikum/wisewoods/#", () => undefined);
    await settle();
    expect(clients[0].subscribed).toEqual([]);

    clients[0].emit("connect");
    expect(clients[0].subscribed).toEqual([T, "steine/v1/_Metric/n-technikum/wisewoods/#"]);
    expect(live.state).toBe("online");
  });

  it("does not guess a username for a token that is not a JWT", async () => {
    const { clients, errors, live } = setup({ token: () => "a-personal-access-token" });
    await settle();

    expect(clients).toHaveLength(0);
    expect(errors[0].message).toMatch(/pass `username`/);
    expect(live.state).toBe("offline");
  });
});

describe("values", () => {
  it("hands a second subscriber the current value without asking the node again", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const first: LiveValue[] = [];
    live.subscribe(T, (value) => first.push(value));
    clients[0].deliver(T, { value: 60, timestamp: 1 }, true);

    const second: LiveValue[] = [];
    live.subscribe(T, (value) => second.push(value));
    await settle();

    expect(clients[0].subscribed).toEqual([T]);
    expect(first.map((v) => v.payload)).toEqual([{ value: 60, timestamp: 1 }]);
    expect(second.map((v) => v.payload)).toEqual([{ value: 60, timestamp: 1 }]);
    expect(second[0].retained).toBe(true);
  });

  it("hands a retained value to each filter once, although the node sends it per subscription", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const wide: unknown[] = [];
    live.subscribe("steine/v1/_Metric/n-technikum/#", (value) => wide.push(value.payload));
    clients[0].deliver(T, { value: 60 }, true);

    const narrow: unknown[] = [];
    live.subscribe(T, (value) => narrow.push(value.payload));
    // The node answers the new subscription with the same retained value again.
    clients[0].deliver(T, { value: 60 }, true);

    expect(wide).toEqual([{ value: 60 }]);
    expect(narrow).toEqual([{ value: 60 }]);
  });

  it("gives a filter subscribed again its retained values again", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const stop = live.subscribe(T, () => undefined);
    clients[0].deliver(T, { value: 60 }, true);
    stop();

    const again: unknown[] = [];
    live.subscribe(T, (value) => again.push(value.payload));
    clients[0].deliver(T, { value: 60 }, true);

    expect(again).toEqual([{ value: 60 }]);
  });

  it("delivers a change to every filter that covers the topic", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const exact: unknown[] = [];
    const wide: unknown[] = [];
    live.subscribe(T, (value) => exact.push(value.payload));
    live.subscribe("steine/v1/_Metric/+/wisewoods/#", (value) => wide.push(value.payload));
    clients[0].deliver(T, { value: 80 });
    clients[0].deliver("steine/v1/_Metric/n-technikum/other/x", { value: 1 });

    expect(exact).toEqual([{ value: 80 }]);
    expect(wide).toEqual([{ value: 80 }]);
    expect(live.latest(T)?.retained).toBe(false);
  });

  it("forgets a path that was retired", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const seen: unknown[] = [];
    live.subscribe(T, (value) => seen.push(value.payload));
    clients[0].deliver(T, { value: 60 }, true);
    clients[0].deliver(T, undefined);

    expect(seen).toEqual([{ value: 60 }, undefined]);
    expect(live.latest(T)).toBeUndefined();
  });

  it("reports what it cannot read, and a listener that throws does not silence the others", async () => {
    const { live, clients, errors } = setup();
    await settle();
    clients[0].emit("connect");

    const seen: unknown[] = [];
    live.subscribe(T, () => {
      throw new Error("a broken widget");
    });
    live.subscribe(T, (value) => seen.push(value.payload));
    clients[0].emit("message", T, new TextEncoder().encode("{not json"), { retain: false });
    clients[0].deliver(T, { value: 1 });

    expect(errors.map((e) => e.message)).toEqual([`unreadable payload on ${T}`, "a broken widget"]);
    expect(seen).toEqual([{ value: 1 }]);
  });
});

describe("unsubscribing", () => {
  it("ends one subscription, and the node's only with the last one", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const a: unknown[] = [];
    const b: unknown[] = [];
    const stopA = live.subscribe(T, (value) => a.push(value.payload));
    const stopB = live.subscribe(T, (value) => b.push(value.payload));

    stopA();
    stopA();
    clients[0].deliver(T, { value: 2 });
    expect(a).toEqual([]);
    expect(b).toEqual([{ value: 2 }]);
    expect(clients[0].unsubscribed).toEqual([]);

    stopB();
    expect(clients[0].unsubscribed).toEqual([T]);
    expect(live.latest(T)).toBeUndefined();
  });

  it("treats the same function subscribed twice as two subscriptions", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const seen: unknown[] = [];
    const listener = (value: LiveValue): void => {
      seen.push(value.payload);
    };
    const stop = live.subscribe(T, listener);
    live.subscribe(T, listener);
    stop();
    clients[0].deliver(T, { value: 3 });

    expect(seen).toEqual([{ value: 3 }]);
  });
});

describe("keeping the connection", () => {
  it("renews before the token runs out and subscribes everything again", async () => {
    const { live, clients, tokens } = setup();
    live.subscribe(T, () => undefined);
    await settle();
    clients[0].emit("connect");

    const states: string[] = [];
    live.onState((state) => states.push(state));

    // 300 s token, renewed 30 s before it expires.
    await vi.advanceTimersByTimeAsync(269_000);
    expect(clients).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1_000);

    expect(clients).toHaveLength(2);
    expect(clients[0].ended).toBe(true);
    expect(clients[1].options.password).toBe(tokens[1]);
    expect(clients[1].options.clientId).not.toBe(clients[0].options.clientId);

    clients[1].emit("connect");
    expect(clients[1].subscribed).toEqual([T]);
    // A planned renewal is not an outage.
    expect(states).toEqual([]);
  });

  it("ignores what the replaced connection still says", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");
    const seen: unknown[] = [];
    live.subscribe(T, (value) => seen.push(value.payload));

    await vi.advanceTimersByTimeAsync(270_000);
    clients[0].deliver(T, { value: "late" });
    clients[0].emit("close");

    expect(seen).toEqual([]);
    expect(live.state).toBe("online");
  });

  it("waits longer after each failed attempt, with a fresh token every time", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    const { live, clients, tokens } = setup({ retryMs: 1_000, maxRetryMs: 4_000 });
    await settle();
    clients[0].emit("connect");

    clients[0].emit("close");
    expect(live.state).toBe("offline");

    await vi.advanceTimersByTimeAsync(1_000);
    expect(clients).toHaveLength(2);
    clients[1].emit("close");

    await vi.advanceTimersByTimeAsync(1_000);
    expect(clients).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(clients).toHaveLength(3);
    clients[2].emit("close");

    // Capped at maxRetryMs.
    await vi.advanceTimersByTimeAsync(4_000);
    expect(clients).toHaveLength(4);
    expect(new Set(tokens).size).toBe(4);

    clients[3].emit("connect");
    expect(live.state).toBe("online");
  });

  it("stops for good when closed", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    live.close();
    clients[0].emit("close");
    await vi.advanceTimersByTimeAsync(600_000);

    expect(clients).toHaveLength(1);
    expect(clients[0].ended).toBe(true);
    expect(live.state).toBe("closed");
    expect(() => live.subscribe(T, () => undefined)).toThrow(/closed/);
  });
});

describe("publishing", () => {
  it("sends JSON at QoS 1 and does not keep anything for later", async () => {
    const { live, clients } = setup();
    await expect(live.publish("steine/v1/_CmdParam/n1/x/setGrit", { a: 1 })).rejects.toThrow(/not connected/);

    await settle();
    clients[0].emit("connect");
    await live.publish("steine/v1/_CmdParam/n1/x/setGrit", { a: 1 });

    expect(clients[0].sent).toEqual([{ topic: "steine/v1/_CmdParam/n1/x/setGrit", body: { a: 1 }, qos: 1 }]);
  });

  it("passes the node's refusal on", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");
    clients[0].refusePublish = new Error("Not authorized");

    await expect(live.publish("steine/v1/_Metric/n1/x", { value: 1 })).rejects.toThrow("Not authorized");
  });

  it("subscribes at the strongest QoS anyone asked for", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    live.subscribe("steine/v1/_Ack/#", () => undefined);
    live.subscribe("steine/v1/_Ack/#", () => undefined, { qos: 1 });

    expect(clients[0].subscribedQos).toEqual([
      ["steine/v1/_Ack/#", 0],
      ["steine/v1/_Ack/#", 1],
    ]);
  });
});

describe("commands", () => {
  const COMMAND = "steine/v1/_CmdParam/n-technikum/wisewoods/line1/mas2/sta1/aggos/setGrit";

  it("waits for the node to confirm the answers' subscription, then sends", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].holdSubacks = true;
    clients[0].emit("connect");

    const answer = live.command(COMMAND, { params: { signal: "grit", value: 120 } });
    await settle();
    expect(clients[0].subscribedQos).toEqual([["steine/v1/_Ack/#", 1]]);
    expect(clients[0].sent).toEqual([]);

    clients[0].grant();
    await settle();
    expect(clients[0].sent).toHaveLength(1);
    const sent = clients[0].sent[0];
    expect(sent.topic).toBe(COMMAND);
    expect(sent.body.params).toEqual({ signal: "grit", value: 120 });
    expect(sent.body.correlation_id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/);
    // Unix milliseconds, 30 s ahead by default.
    expect(sent.body.expires_at).toBe(Date.now() + 30_000);

    // Somebody else's answer first, then ours from wherever the executor sits.
    clients[0].deliver("steine/v1/_Ack/n-edge/x/setGrit", { correlation_id: "01OTHER", result_code: 200 });
    clients[0].deliver("steine/v1/_Ack/n-edge/wisewoods/line1/mas2/sta1/aggos/setGrit", {
      correlation_id: sent.body.correlation_id,
      result_code: 200,
      message: "written",
    });

    await expect(answer).resolves.toMatchObject({ result_code: 200, message: "written" });
  });

  it("settles with a refusal too, and rejects only when nobody answers", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const refused = live.command(COMMAND, {}, { timeoutMs: 5_000 });
    const silent = live.command(COMMAND, {}, { timeoutMs: 5_000 });
    await settle();
    const [first] = clients[0].sent;
    clients[0].deliver("steine/v1/_Ack/n-edge/x", {
      correlation_id: first.body.correlation_id,
      result_code: 403,
    });

    await expect(refused).resolves.toMatchObject({ result_code: 403 });
    const late = expect(silent).rejects.toBeInstanceOf(CommandTimeout);
    await vi.advanceTimersByTimeAsync(5_000);
    await late;
    // One subscription to the answers for both.
    expect(clients[0].subscribed).toEqual(["steine/v1/_Ack/#"]);
  });

  it("refuses a topic that is not a command, and fails when the publish is refused", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    await expect(live.command("steine/v1/_Metric/n1/x")).rejects.toThrow(/_Cmd/);

    clients[0].refusePublish = new Error("Not authorized");
    await expect(live.command(COMMAND)).rejects.toThrow("Not authorized");
  });

  it("gives up on open commands when closed", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");

    const answer = live.command(COMMAND);
    await settle();
    live.close();

    await expect(answer).rejects.toThrow(/closed/);
  });
});
