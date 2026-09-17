import { describe, expect, it } from "vitest";

import { Door, DoorError } from "../src/door.js";

/** A `fetch` that answers from a script and records what it was asked. */
function stub(answers: { status?: number; body: unknown }[]): {
  fetch: typeof globalThis.fetch;
  calls: { url: string; method: string; body: unknown }[];
} {
  const calls: { url: string; method: string; body: unknown }[] = [];
  let turn = 0;
  const fetch = ((url: string, init: RequestInit) => {
    calls.push({
      url,
      method: init.method ?? "GET",
      body: typeof init.body === "string" ? (JSON.parse(init.body) as unknown) : undefined,
    });
    const answer = answers[Math.min(turn, answers.length - 1)];
    turn += 1;
    return Promise.resolve(
      new Response(JSON.stringify(answer.body), {
        status: answer.status ?? 200,
        headers: { "content-type": "application/json" },
      }),
    );
  }) as unknown as typeof globalThis.fetch;
  return { fetch, calls };
}

const record = {
  offset: 7,
  origin_offset: 7,
  topic: "steine/v1/_Annotation/n-technikum/panel/01M2",
  payload: { annotation_id: "01M2" },
  ts: 1_789_535_138_047,
  written_by: "dataops",
  actor_id: "01ABC",
  actor_label: "dataops",
  actor_kind: "service",
};

describe("fetch", () => {
  it("names the stream, the cursor and the extras the node understands", async () => {
    const { fetch, calls } = stub([{ body: { records: [record], next: 8 } }]);
    const door = new Door({ baseUrl: "http://colca/", service: "my-app", fetch });

    const page = await door.fetchPage("metrics", door.cursorName("live"), {
      max: 50,
      prefix: "wisewoods/line1",
      signalIds: ["01S1", "01S2"],
      tail: true,
    });

    const query = new URL(calls[0].url).searchParams;
    expect(query.get("stream")).toBe("metrics");
    expect(query.get("cursor")).toBe("c/my-app/live");
    expect(query.get("max")).toBe("50");
    expect(query.get("prefix")).toBe("wisewoods/line1");
    expect(query.get("tail")).toBe("1");
    expect(query.getAll("signal_id")).toEqual(["01S1", "01S2"]);
    expect(page.records[0].originOffset).toBe(7);
    expect(page.records[0].writtenBy).toBe("dataops");
    expect(page.next).toBe(8);
  });

  it("carries a gap through", async () => {
    const { fetch } = stub([
      {
        body: {
          records: [],
          next: 99,
          gap: { stream: "metrics", from_offset: 1, to_offset: 98, approx: true },
        },
      },
    ]);
    const door = new Door({ baseUrl: "http://colca", service: "my-app", fetch });

    const page = await door.fetchPage("metrics", "c/my-app/live");
    expect(page.gap).toEqual({
      stream: "metrics",
      fromOffset: 1,
      toOffset: 98,
      firstTs: undefined,
      lastTs: undefined,
      approx: true,
    });
  });
});

describe("publish", () => {
  it("leaves unset attribution off the wire", async () => {
    const { fetch, calls } = stub([{ body: { stream: "metrics", offset: 12, topic: "t" } }]);
    const door = new Door({ baseUrl: "http://colca", service: "my-app", fetch });

    await door.publish("steine/v1/_Metric/n-technikum/probe/x", { value: 1 });

    expect(calls[0].body).toEqual({ topic: "steine/v1/_Metric/n-technikum/probe/x", payload: { value: 1 } });
  });

  it("keeps the node's denial reason", async () => {
    const { fetch } = stub([{ status: 403, body: { error: "not your zone", reason: "zone" } }]);
    const door = new Door({ baseUrl: "http://colca", service: "my-app", fetch });

    const failure = await door.publish("steine/v1/_Metric/other/x", {}).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(DoorError);
    expect((failure as DoorError).status).toBe(403);
    expect((failure as DoorError).reason).toBe("zone");
  });
});

describe("kv", () => {
  it("follows the pages and stops when the token repeats", async () => {
    const entry = { path: "a", node_id: "n", topic: "t", payload: {}, ts: 1, offset: 1 };
    const { fetch, calls } = stub([
      { body: { entries: [entry], next: "page2" } },
      { body: { entries: [entry], next: "" } },
    ]);
    const door = new Door({ baseUrl: "http://colca", service: "my-app", fetch });

    const entries = await door.kv("plant/", { contract: ["_Signal", "_Group"] });

    expect(entries).toHaveLength(2);
    expect(entries[0].nodeId).toBe("n");
    expect(new URL(calls[0].url).searchParams.getAll("contract")).toEqual(["_Signal", "_Group"]);
    expect(new URL(calls[1].url).searchParams.get("after")).toBe("page2");
  });

  it("refuses a node that keeps handing back the same page", async () => {
    const { fetch } = stub([{ body: { entries: [], next: "same" } }]);
    const door = new Door({ baseUrl: "http://colca", service: "my-app", fetch });

    await expect(door.kv()).rejects.toThrow("repeated a page token");
  });
});

describe("cursors", () => {
  it("needs a service name to build one", () => {
    const { fetch } = stub([{ body: {} }]);
    const door = new Door({ baseUrl: "https://node:443", token: "jwt", fetch });
    expect(() => door.cursorName("live")).toThrow(/service name/);
  });
});
