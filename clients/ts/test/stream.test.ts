import { afterEach, describe, expect, it, vi } from "vitest";

import { Door, type Gap } from "../src/door.js";
import { Doorbell, IDLE_DRAIN_MS } from "../src/doorbell.js";
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
  afterEach(() => {
    vi.useRealTimers();
  });

  /** Runs `follow` to its end and collects the offsets it yields. */
  function consume(records: AsyncGenerator<{ offset: number }>): { seen: number[]; done: Promise<void> } {
    const seen: number[] = [];
    const done = (async () => {
      for await (const item of records) seen.push(item.offset);
    })();
    return { seen, done };
  }

  it("drains at once, then at a ring without waiting, and a ring during a drain is not lost", async () => {
    vi.useFakeTimers();
    const { door, fetches } = node([{ records: [], next: 1, from: 1 }]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));
    const bell = new Doorbell();
    const stop = new AbortController();

    const { done } = consume(stream.follow({ bell, signal: stop.signal }));
    await vi.advanceTimersByTimeAsync(0);
    expect(fetches).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(IDLE_DRAIN_MS - 1);
    expect(fetches).toHaveLength(1); // a silent bell is not a polling cadence

    bell.ring();
    bell.ring();
    await vi.advanceTimersByTimeAsync(0);
    expect(fetches).toHaveLength(2); // two rings, one drain, no time passed

    stop.abort();
    await done;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("reads again from its cursor when no ring came within the bound, and yields what it missed", async () => {
    vi.useFakeTimers();
    const { door, fetches, acks } = node([
      { records: [], next: 1 },
      { records: [record(1), record(2)], next: 3 },
      { records: [], next: 3 },
    ]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));
    const stop = new AbortController();

    // The ring for records 1 and 2 was lost: nothing ever rings this bell.
    const { seen, done } = consume(stream.follow({ bell: new Doorbell(), signal: stop.signal }));
    await vi.advanceTimersByTimeAsync(IDLE_DRAIN_MS - 1);
    expect(fetches).toHaveLength(1);
    expect(seen).toEqual([]);

    await vi.advanceTimersByTimeAsync(1);
    expect(seen).toEqual([1, 2]);
    expect(acks.map((a) => a.offset)).toEqual([2]);

    stop.abort();
    await done;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("takes its own bound", async () => {
    vi.useFakeTimers();
    const { door, fetches } = node([{ records: [], next: 1 }]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));
    const stop = new AbortController();

    const { done } = consume(stream.follow({ bell: new Doorbell(), signal: stop.signal, idleDrainMs: 1000 }));
    await vi.advanceTimersByTimeAsync(999);
    expect(fetches).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetches).toHaveLength(2);

    stop.abort();
    await done;
  });

  it("waits for a ring alone when the bound is null", async () => {
    vi.useFakeTimers();
    const { door, fetches } = node([{ records: [], next: 1 }]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));
    const bell = new Doorbell();
    const stop = new AbortController();

    const { done } = consume(stream.follow({ bell, signal: stop.signal, idleDrainMs: null }));
    await vi.advanceTimersByTimeAsync(10 * IDLE_DRAIN_MS);
    expect(fetches).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);

    bell.ring();
    await vi.advanceTimersByTimeAsync(0);
    expect(fetches).toHaveLength(2);

    stop.abort();
    await done;
  });

  it("refuses a bound that would read in a loop", async () => {
    const { door, fetches } = node([{ records: [], next: 1 }]);
    const stream = new Stream(door, "annotations", door.cursorName("panels"));

    for (const idleDrainMs of [0, -1, Number.NaN]) {
      const records = stream.follow({ bell: new Doorbell(), idleDrainMs });
      await expect(records.next()).rejects.toThrow(RangeError);
    }
    expect(fetches).toHaveLength(0);
  });
});

describe("Doorbell", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("resolves at once for a ring after the generation was taken", async () => {
    const bell = new Doorbell();
    const seen = bell.generation;
    bell.ring();
    await expect(bell.after(seen)).resolves.toBe("rang");
    expect(bell.generation).toBe(seen + 1);
  });

  it("says a ring woke it, and leaves no timer behind", async () => {
    vi.useFakeTimers();
    const bell = new Doorbell();
    const woke = bell.after(bell.generation, { timeoutMs: 1000 });
    expect(vi.getTimerCount()).toBe(1);

    bell.ring();
    await expect(woke).resolves.toBe("rang");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("says the bound woke it when nothing rang", async () => {
    vi.useFakeTimers();
    const bell = new Doorbell();
    let woke: string | undefined;
    void bell.after(bell.generation, { timeoutMs: 1000 }).then((why) => (woke = why));

    await vi.advanceTimersByTimeAsync(999);
    expect(woke).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(woke).toBe("timeout");

    // The waiter is gone with its timer: a later ring has nobody left to wake twice.
    bell.ring();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("says an abort woke it, with the signal as an option or as the argument itself", async () => {
    vi.useFakeTimers();
    const bell = new Doorbell();

    const stop = new AbortController();
    const woke = bell.after(bell.generation, { signal: stop.signal, timeoutMs: 1000 });
    stop.abort();
    await expect(woke).resolves.toBe("aborted");
    expect(vi.getTimerCount()).toBe(0);

    const positional = new AbortController();
    const wokeToo = bell.after(bell.generation, positional.signal);
    positional.abort();
    await expect(wokeToo).resolves.toBe("aborted");

    await expect(bell.after(bell.generation, stop.signal)).resolves.toBe("aborted");
  });

  it("refuses a bound that would wake it in a loop", async () => {
    const bell = new Doorbell();
    await expect(bell.after(bell.generation, { timeoutMs: 0 })).rejects.toThrow(RangeError);
  });

  it("waits without a bound when none is given", async () => {
    vi.useFakeTimers();
    const bell = new Doorbell();
    let woke: string | undefined;
    void bell.after(bell.generation).then((why) => (woke = why));

    await vi.advanceTimersByTimeAsync(24 * 3600 * 1000);
    expect(woke).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    bell.ring();
    await vi.advanceTimersByTimeAsync(0);
    expect(woke).toBe("rang");
  });

  it("holds a bound longer than a timer can run instead of firing at once", async () => {
    vi.useFakeTimers();
    const bell = new Doorbell();
    let woke: string | undefined;
    void bell.after(bell.generation, { timeoutMs: 2 ** 40 }).then((why) => (woke = why));

    await vi.advanceTimersByTimeAsync(1000);
    expect(woke).toBeUndefined();
    bell.ring();
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
