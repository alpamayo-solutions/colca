import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AlarmRefused, Alarms, type AlarmState } from "../src/alarms.js";
import { CommandTimeout, type Live } from "../src/live.js";
import { FakeClient, settle, setup } from "./fake-mqtt.js";

const ROOT = "steine";
const NODE = "n-technikum";
const FILTER = `${ROOT}/v1/_AlarmState/${NODE}/#`;
const GRIT = "wisewoods/line1/mas2/gritLow";
const DOOR = "wisewoods/line1/mas2/doorOpen";
const TEMP = "wisewoods/line1/mas3/tempHigh";

function view(live: Live): Alarms {
  return new Alarms({ live, node: NODE, root: ROOT });
}

function state(over: Partial<AlarmState> = {}): AlarmState {
  return {
    alarm_id: "01ALARM",
    status: "firing",
    severity: "warning",
    since: 1_700_000_000,
    signal_id: "01SIGNAL",
    reason: "grit below 40",
    ...over,
  };
}

/** The node's retained `_AlarmState` record for one alarm. */
function raise(client: FakeClient, path: string, over: Partial<AlarmState> = {}): void {
  client.deliver(`${ROOT}/v1/_AlarmState/${NODE}/${path}`, state(over), true);
}

/** The alarm going: an empty payload over the retained record. */
function withdraw(client: FakeClient, path: string): void {
  client.deliver(`${ROOT}/v1/_AlarmState/${NODE}/${path}`, undefined);
}

/** A connected client with its alarms subscribed. */
async function opened(): Promise<{ live: Live; client: FakeClient; alarms: Alarms }> {
  const { live, clients } = setup();
  await settle();
  clients[0].emit("connect");
  return { live, client: clients[0], alarms: view(live) };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-09-17T08:00:00Z"));
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("what stands", () => {
  it("has the node's whole set as soon as the retained records arrive", async () => {
    const { client, alarms } = await opened();
    expect(client.subscribed).toEqual([FILTER]);

    raise(client, GRIT, { severity: "critical", since: 100 });
    raise(client, DOOR, { severity: "info", since: 50 });

    expect(alarms.standing().map((alarm) => alarm.path)).toEqual([GRIT, DOOR]);
    expect(alarms.standing()[0].topic).toBe(`${ROOT}/v1/_AlarmState/${NODE}/${GRIT}`);
    expect(alarms.standing()[0].state.reason).toBe("grit below 40");
  });

  it("drops an alarm that was withdrawn with an empty payload", async () => {
    const { client, alarms } = await opened();
    raise(client, GRIT);
    raise(client, DOOR);
    expect(alarms.standing()).toHaveLength(2);

    withdraw(client, GRIT);

    expect(alarms.standing().map((alarm) => alarm.path)).toEqual([DOOR]);
  });

  it("puts the worst first, and the oldest first within a severity", async () => {
    const { client, alarms } = await opened();
    raise(client, DOOR, { severity: "info", since: 10 });
    raise(client, GRIT, { severity: "critical", since: 300 });
    raise(client, TEMP, { severity: "critical", since: 200 });
    raise(client, "wisewoods/line1/mas3/beltSlow", { severity: "warning", since: 20 });

    expect(alarms.standing().map((alarm) => alarm.path)).toEqual([
      TEMP,
      GRIT,
      "wisewoods/line1/mas3/beltSlow",
      DOOR,
    ]);
  });

  it("leaves out what is below the severity asked for", async () => {
    const { client, alarms } = await opened();
    raise(client, DOOR, { severity: "info" });
    raise(client, GRIT, { severity: "warning" });
    raise(client, TEMP, { severity: "critical" });

    expect(alarms.standing({ minSeverity: "warning" }).map((alarm) => alarm.path)).toEqual([TEMP, GRIT]);
    expect(alarms.standing({ minSeverity: "critical" }).map((alarm) => alarm.path)).toEqual([TEMP]);
  });
});

describe("watching", () => {
  it("tells a watcher what stands now and again on every change", async () => {
    const { client, alarms } = await opened();
    raise(client, GRIT, { severity: "critical" });

    const seen: string[][] = [];
    alarms.onChange((standing) => seen.push(standing.map((alarm) => alarm.path)));
    await settle();
    expect(seen).toEqual([[GRIT]]);

    raise(client, DOOR, { severity: "info" });
    withdraw(client, GRIT);

    expect(seen).toEqual([[GRIT], [GRIT, DOOR], [DOOR]]);
  });

  it("stops one watcher and no other", async () => {
    const { client, alarms } = await opened();

    const one: unknown[] = [];
    const two: unknown[] = [];
    const stop = alarms.onChange((standing) => one.push(standing.length));
    alarms.onChange((standing) => two.push(standing.length), { minSeverity: "critical" });
    await settle();

    stop();
    raise(client, GRIT, { severity: "critical" });

    expect(one).toEqual([0]);
    expect(two).toEqual([0, 1]);
  });

  it("ends the node's subscription when closed", async () => {
    const { client, alarms } = await opened();
    alarms.close();

    expect(client.unsubscribed).toEqual([FILTER]);
  });
});

describe("acknowledging", () => {
  const ACK = `${ROOT}/v1/_Ack/${NODE}/${GRIT}/ackAlarm`;

  it("commands the alarm's own path and sends the note, and no identity", async () => {
    const { client, alarms } = await opened();

    const quit = alarms.acknowledge(GRIT, { note: "Korn getauscht" });
    await settle();

    const sent = client.sent[0];
    expect(sent.topic).toBe(`${ROOT}/v1/_CmdOperate/${NODE}/${GRIT}/ackAlarm`);
    // Who quit it is the node's word; the client says only why.
    expect(Object.keys(sent.body).sort()).toEqual(["correlation_id", "expires_at", "note"]);
    expect(sent.body.note).toBe("Korn getauscht");
    expect(sent.body.expires_at).toBe(Date.now() + 30_000);

    client.deliver(ACK, { correlation_id: sent.body.correlation_id, result_code: 200, message: "quit" });
    await expect(quit).resolves.toMatchObject({ result_code: 200, message: "quit" });
  });

  it("throws what the node refused rather than looking quit", async () => {
    const { client, alarms } = await opened();

    const quit = alarms.acknowledge(GRIT);
    await settle();
    client.deliver(ACK, {
      correlation_id: client.sent[0].body.correlation_id,
      result_code: 403,
      message: "not yours to quit",
    });

    await expect(quit).rejects.toBeInstanceOf(AlarmRefused);
    await expect(quit).rejects.toThrow(/403 not yours to quit/);
  });

  it("throws when nobody answers", async () => {
    const { client, alarms } = await opened();

    const quit = alarms.acknowledge(GRIT, { timeoutMs: 5_000 });
    const late = expect(quit).rejects.toBeInstanceOf(CommandTimeout);
    await settle();
    expect(client.sent[0].body.expires_at).toBe(Date.now() + 5_000);

    await vi.advanceTimersByTimeAsync(5_000);
    await late;
  });
});

describe("silencing", () => {
  it("sends the deadline it was given", async () => {
    const { client, alarms } = await opened();

    void alarms.silence(GRIT, { until: 1_700_003_600, note: "Wartung" });
    await settle();

    expect(client.sent[0].topic).toBe(`${ROOT}/v1/_CmdOperate/${NODE}/${GRIT}/silenceAlarm`);
    expect(client.sent[0].body).toMatchObject({ until: 1_700_003_600, note: "Wartung" });
  });

  it("turns minutes into a deadline in unix seconds", async () => {
    const { client, alarms } = await opened();

    void alarms.silence(GRIT, { minutes: 30 });
    await settle();

    expect(client.sent[0].body.until).toBe(Math.round(Date.now() / 1000) + 1_800);
    expect(client.sent[0].body.note).toBeUndefined();
  });

  it("wants one of the two", async () => {
    const { alarms } = await opened();

    await expect(alarms.silence(GRIT, {})).rejects.toThrow(/`until`.*`minutes`/);
  });

  it("lets it notify again", async () => {
    const { client, alarms } = await opened();

    void alarms.unsilence(GRIT);
    await settle();

    expect(client.sent[0].topic).toBe(`${ROOT}/v1/_CmdOperate/${NODE}/${GRIT}/unsilenceAlarm`);
  });
});
