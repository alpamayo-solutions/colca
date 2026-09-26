import { describe, expect, it } from "vitest";

import { Door, type Gap } from "../src/door.js";
import { Doorbell } from "../src/doorbell.js";
import { Stream } from "../src/stream.js";

interface Ack {
  cursor: string;
  stream: string;
  offset?: number;
  delete?: boolean;
}

/** A node that answers `/fetch` from a script and remembers every ack. */
function node(pages: unknown[]): { door: Door; acks: Ack[]; fetches: URL[] } {
  const acks: Ack[] = [];
  const fetches: URL[] = [];
  let turn = 0;
  const fetch = ((url: string, init: RequestInit) => {
    const target = new URL(url);
    let body: unknown;
    if (target.pathname === "/ack") {
      acks.push(JSON.parse(typeof init.body === "string" ? init.body : "{}") as Ack);
      body = { moved: true };
    } else {
      fetches.push(target);
      body = pages[Math.min(turn, pages.length - 1)];
      turn += 1;
    }
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
  }) as unknown as typeof globalThis.fetch;

  return { door: new Door({ baseUrl: "http://colca", service: "my-app", fetch }), acks, fetches };
}

function record(offset: number): Record<string, unknown> {
  return { offset, origin_offset: offset, topic: "t", payload: {}, ts: offset * 1000 };
}

describe("drain", () => {
  it("yields every record and acks each page once it is through", async () => {
    const { door, acks } = node([
      { records: [record(1), record(2)], next: 3 },
      { records: [record(3)], next: 4 },
      { records: [], next: 4 },
    ]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));

    const seen: number[] = [];
    for await (const item of stream.drain()) seen.push(item.offset);

    expect(seen).toEqual([1, 2, 3]);
    expect(acks.map((a) => a.offset)).toEqual([2, 3]);
    expect(acks[0].cursor).toBe("c/my-app/panels");
  });

  it("acks past a hole so the same gap is not served forever", async () => {
    const gaps: Gap[] = [];
    const { door, acks } = node([
      { records: [], next: 98, gap: { stream: "annotations", from_offset: 1, to_offset: 97, approx: false } },
      { records: [], next: 98 },
    ]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"), {
      onGap: (gap) => gaps.push(gap),
    });

    const seen: number[] = [];
    for await (const item of stream.drain()) seen.push(item.offset);

    expect(seen).toEqual([]);
    expect(gaps.map((g) => g.toOffset)).toEqual([97]);
    expect(acks.map((a) => a.offset)).toEqual([97]);
  });
});

describe("drain past filtered records", () => {
  it("acks up to next, so records the filter skipped do not wait on the cursor", async () => {
    const { door, acks, fetches } = node([
      { records: [record(3)], next: 9, from: 1 },
      { records: [], next: 9, from: 9 },
    ]);
    const stream = new Stream(door, "commands", door.cursorName("commands"), {
      topics: ["colca/v1/_CmdSet/n/mine/#"],
    });

    const seen: number[] = [];
    for await (const item of stream.drain()) seen.push(item.offset);

    expect(seen).toEqual([3]);
    expect(acks.map((a) => a.offset)).toEqual([8]);
    expect(fetches[0].searchParams.getAll("topic")).toEqual(["colca/v1/_CmdSet/n/mine/#"]);
  });
});

describe("follow", () => {
  it("drains at once, then only after a ring, and a ring during a drain is not lost", async () => {
    const { door, fetches } = node([{ records: [], next: 1, from: 1 }]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));
    const bell = new Doorbell();
    const stop = new AbortController();

    const run = (async () => {
      for await (const item of stream.follow({ bell, signal: stop.signal })) expect(item).toBeDefined();
    })();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(fetches).toHaveLength(1);

    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(fetches).toHaveLength(1); // nothing on a timer

    bell.ring();
    bell.ring();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(fetches).toHaveLength(2); // two rings, one drain

    stop.abort();
    await run;
  });
});

describe("Doorbell", () => {
  it("resolves at once for a ring after the generation was taken", async () => {
    const bell = new Doorbell();
    const seen = bell.generation;
    bell.ring();
    await bell.after(seen);
    expect(bell.generation).toBe(seen + 1);
  });
});

describe("tail", () => {
  it("reads the end of the stream and leaves the cursor where it was", async () => {
    const { door, acks, fetches } = node([{ records: [record(9)], next: 10 }]);
    const stream = new Stream(door, "annotations", door.cursorName("view"), { max: 5 });

    const records = await stream.tail();

    expect(records.map((r) => r.offset)).toEqual([9]);
    expect(fetches[0].searchParams.get("tail")).toBe("1");
    expect(fetches[0].searchParams.get("max")).toBe("5");
    expect(acks).toEqual([]);
  });
});

describe("retire", () => {
  it("deletes the cursor", async () => {
    const { door, acks } = node([{ records: [], next: 1 }]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));

    await stream.retire();

    expect(acks).toEqual([{ cursor: "c/my-app/panels", stream: "annotations", delete: true }]);
  });
});
