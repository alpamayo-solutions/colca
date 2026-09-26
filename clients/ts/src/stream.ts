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
import type { Doorbell } from "./doorbell.js";

export interface StreamOptions {
  /** Records per page, 1…1000. */
  max?: number;
  /** Narrows to a subtree of the UNS path. */
  prefix?: string;
  /** Only for the `metrics` stream. */
  signalIds?: readonly string[];
  /**
   * MQTT topic filters: keeps records whose topic matches one. A consumer woken
   * by a set of topics reads exactly that set, and the node counts only those
   * records as unread for this cursor.
   */
  topics?: readonly string[];
  /**
   * Called when a page reports pruned records. Nothing else announces it — a
   * consumer that cares about completeness should say so here.
   */
  onGap?: (gap: Gap) => void;
}

export interface FollowOptions {
  /**
   * Rung whenever the stream may have grown: by an MQTT subscription to the
   * topics this stream reads, a `/watch` hint, and every reconnect.
   */
  bell: Doorbell;
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

      const offset = ackOffset(page);
      if (offset === undefined) return;
      await this.ack(offset);
    }
  }

  /**
   * Drain now, then again after every ring of `bell`, until `signal` aborts.
   * Nothing is read on a timer: a ring during a drain leads to one more drain,
   * and a consumer rings the bell on reconnect so what arrived meanwhile is read.
   */
  async *follow({ bell, signal }: FollowOptions): AsyncGenerator<DoorRecord> {
    while (!signal?.aborted) {
      const seen = bell.generation;
      yield* this.drain(signal);
      await bell.after(seen, signal);
    }
  }

  #fetchOptions(signal?: AbortSignal): FetchOptions {
    return {
      max: this.options.max,
      prefix: this.options.prefix,
      signalIds: this.options.signalIds,
      topics: this.options.topics,
      signal,
    };
  }
}

/**
 * What to ack once `page` is processed: its last record; past the records the
 * filter skipped when the node reported where the page started, so they do not
 * count as unread; the gap's bound when a hole swallowed the whole page, so
 * the same gap is not reported forever.
 */
function ackOffset(page: Page): number | undefined {
  const candidates = [page.records.at(-1)?.offset, page.gap?.toOffset];
  if (page.start !== undefined && page.next > page.start) candidates.push(page.next - 1);
  const known = candidates.filter((offset): offset is number => offset !== undefined);
  return known.length === 0 ? undefined : Math.max(...known);
}
