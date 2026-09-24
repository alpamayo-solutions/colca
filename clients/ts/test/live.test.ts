import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CommandNotSent, CommandTimeout, type LiveValue } from "../src/live.js";
import { jwt, settle, setup } from "./fake-mqtt.js";

const T = "steine/v1/_Metric/n-technikum/wisewoods/line1/mas2/grit";

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

  it("keeps the connection while the token on offer is no newer than the one in use", async () => {
    // A proxy that goes on handing out the token it holds until its own refresh is due.
    const exp = Math.floor(Date.now() / 1000) + 300;
    const stale = jwt({ sub: "till", exp });
    const fresh = jwt({ sub: "till", exp: exp + 300 });
    let fetches = 0;
    const { live, clients } = setup({
      token: () => {
        fetches += 1;
        return fetches < 4 ? stale : fresh;
      },
    });
    live.subscribe(T, () => undefined);
    await settle();
    clients[0].emit("connect");

    // Due 30 s before expiry: twice the same token, five seconds apart, and no new connection.
    await vi.advanceTimersByTimeAsync(270_000);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(fetches).toBe(3);
    expect(clients).toHaveLength(1);
    expect(clients[0].ended).toBe(false);

    await vi.advanceTimersByTimeAsync(5_000);
    expect(fetches).toBe(4);
    expect(clients).toHaveLength(2);
    expect(clients[1].options.password).toBe(fresh);
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

  it("says when every subscription has gone out on a new connection", async () => {
    const { live, clients } = setup();
    live.subscribe(T, () => undefined);
    let rounds = 0;
    const stop = live.onResubscribe(() => {
      rounds += 1;
    });
    await settle();

    clients[0].emit("connect");
    expect(rounds).toBe(1);

    // A planned renewal starts the node's retained delivery over as well.
    await vi.advanceTimersByTimeAsync(270_000);
    clients[1].emit("connect");
    expect(rounds).toBe(2);
    expect(clients[1].subscribed).toEqual([T]);

    stop();
    clients[1].emit("close");
    await vi.advanceTimersByTimeAsync(1_000);
    clients[2].emit("connect");
    expect(rounds).toBe(2);
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

    // mqtt.js hands over the node's refusal with its reason code.
    clients[0].refusePublish = Object.assign(new Error("Publish error: Not authorized"), { code: 135 });
    const refused = live.command(COMMAND);
    await expect(refused).rejects.toBeInstanceOf(CommandNotSent);
    await expect(refused).rejects.toMatchObject({ cause: { message: "Publish error: Not authorized" } });
  });

  it("says a command was not sent when it never went out", async () => {
    const { live } = setup();
    await settle();

    const offline = live.command(COMMAND, {}, { timeoutMs: 5_000 });
    const outcome = expect(offline).rejects.toBeInstanceOf(CommandNotSent);
    await vi.advanceTimersByTimeAsync(5_000);
    await outcome;
  });

  it("keeps waiting for the answer when the connection drops after the command went out", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");
    clients[0].refusePublish = new Error("Connection closed");

    const answer = live.command(COMMAND, {}, { timeoutMs: 5_000 });
    await settle();
    const [sent] = clients[0].sent;
    clients[0].emit("close");
    await vi.advanceTimersByTimeAsync(1_000);
    await settle();
    const again = clients[clients.length - 1];
    again.emit("connect");
    again.deliver("steine/v1/_Ack/n-edge/x", { correlation_id: sent.body.correlation_id, result_code: 200 });

    await expect(answer).resolves.toMatchObject({ result_code: 200 });
  });

  it("times out, not 'not sent', when a command went out and its answer never came", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].emit("connect");
    clients[0].refusePublish = new Error("Connection closed");

    const answer = live.command(COMMAND, {}, { timeoutMs: 5_000 });
    const outcome = expect(answer).rejects.toBeInstanceOf(CommandTimeout);
    await vi.advanceTimersByTimeAsync(5_000);
    await outcome;
  });

  it("expires when its wait ends, however late the node confirms the answers' subscription", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].holdSubacks = true;
    clients[0].emit("connect");

    const asked = Date.now();
    const answer = live.command(COMMAND, {}, { timeoutMs: 5_000 });
    void answer.catch(() => undefined);
    await vi.advanceTimersByTimeAsync(2_000);
    clients[0].grant();
    await settle();

    expect(clients[0].sent[0].body.expires_at).toBe(asked + 5_000);
  });

  it("is not sent once its wait has ended", async () => {
    const { live, clients } = setup();
    await settle();
    clients[0].holdSubacks = true;
    clients[0].emit("connect");

    const answer = live.command(COMMAND, {}, { timeoutMs: 5_000 });
    const outcome = expect(answer).rejects.toBeInstanceOf(CommandNotSent);
    await vi.advanceTimersByTimeAsync(5_000);
    await outcome;
    clients[0].grant();
    await settle();

    expect(clients[0].sent).toEqual([]);
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
