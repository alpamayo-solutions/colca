/**
 * A named cursor over one stream.
 *
 * The cursor lives at the node: `fetch` reads from its stored position and
 * only `ack` moves it. Reopening the same name resumes where it was acked; a
 * new name starts at the first record still retained.
 *
 * `drain()` acks a page once the consumer has asked for the record after its
 * last one. A consumer that throws mid-page sees that page again, so handlers
 * must be able to run twice on the same record.
 */

import type { Door, DoorRecord, FetchOptions, Gap, Page } from "./door.js";

export interface StreamOptions {
  /** Records per page, 1…1000. */
  max?: number;
  /** Narrows to a subtree of the UNS path. */
  prefix?: string;
  /** Only for the `metrics` stream. */
  signalIds?: readonly string[];
  /**
   * Called when a page reports pruned records. Nothing else announces it — a
   * consumer that cares about completeness should say so here.
   */
  onGap?: (gap: Gap) => void;
}

export interface FollowOptions {
  /** How long to wait after an empty page. */
  pollMs?: number;
  signal?: AbortSignal;
}

export class Stream {
  constructor(
    private readonly door: Door,
    readonly name: string,
    readonly cursor: string,
    private readonly options: StreamOptions = {},
  ) {}

  /** One page from the cursor's position. Never moves the cursor. */
  async page(signal?: AbortSignal): Promise<Page> {
    return this.door.fetchPage(this.name, this.cursor, this.#fetchOptions(signal));
  }

  /** Ack `upto` — a record or its offset — as the last processed position. */
  async ack(upto: DoorRecord | number): Promise<boolean> {
    return this.door.ack(this.name, this.cursor, typeof upto === "number" ? upto : upto.offset);
  }

  /** Delete this cursor at the node. Fine when it never existed. */
  async retire(): Promise<void> {
    await this.door.deleteCursor(this.name, this.cursor);
  }

  /**
   * The newest records on the stream, without touching the cursor — what a
   * view wants when it opens. A consumer and a view can share a cursor name.
   */
  async tail(max?: number): Promise<DoorRecord[]> {
    const page = await this.door.fetchPage(this.name, this.cursor, {
      ...this.#fetchOptions(),
      max: max ?? this.options.max,
      tail: true,
    });
    return page.records;
  }

  /** Every record from the cursor to the head, acking page by page. Ends at the first empty page. */
  async *drain(signal?: AbortSignal): AsyncGenerator<DoorRecord> {
    for (;;) {
      const page = await this.page(signal);
      if (page.gap) this.options.onGap?.(page.gap);
      yield* page.records;

      // The gap's bound when the hole swallowed the whole page, so the same
      // gap is not reported forever.
      const offset = page.records.at(-1)?.offset ?? page.gap?.toOffset;
      if (offset === undefined) return;
      await this.ack(offset);
    }
  }

  /** `drain()` without an end: after an empty page, wait and drain again. */
  async *follow({ pollMs = 1000, signal }: FollowOptions = {}): AsyncGenerator<DoorRecord> {
    while (!signal?.aborted) {
      yield* this.drain(signal);
      await sleep(pollMs, signal);
    }
  }

  #fetchOptions(signal?: AbortSignal): FetchOptions {
    return { max: this.options.max, prefix: this.options.prefix, signalIds: this.options.signalIds, signal };
  }
}

/** Resolves early when the signal aborts, so `follow()` stops within a poll. */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
    function done(): void {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    }
  });
}
