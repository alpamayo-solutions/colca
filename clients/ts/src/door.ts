/**
 * The door's HTTP client: `GET /fetch`, `POST /ack`, `GET /kv`, `GET /self`
 * and `POST /publish`. One class, no runtime dependencies — the platform's
 * `fetch` does the work.
 *
 * Which door a client talks to is decided by the URL and the credential:
 *
 * - the local door (`http://colca`, port 80 inside the deployment network) has
 *   no credential. The caller names itself with `X-Colca-Service` and the door
 *   registers it on first sight;
 * - the published door (`https://node:443`) wants a credential: a person's
 *   bearer token, or a machine's pinned client certificate — the latter is a
 *   TLS matter and therefore the runtime's, not this client's (see `agent`).
 *
 * Wire fields arrive in snake_case and are handed on in camelCase; the payload
 * itself is passed through untouched, because decoding it into a contract type
 * is the caller's business.
 */

import { topic as buildTopic, type TopicOptions } from "./topics.js";

export interface DoorRecord {
  offset: number;
  originOffset: number;
  topic: string;
  payload: unknown;
  /** Colca's record timestamp, in unix **milliseconds** (payload times are seconds). */
  ts: number;
  writtenBy: string;
  actorId: string;
  actorLabel: string;
  actorKind: string;
}

/**
 * A pruned range. Present only when the cursor sits below the stream's
 * low-water mark, and reported again until the consumer acks `toOffset`.
 */
export interface Gap {
  stream: string;
  fromOffset: number;
  toOffset: number;
  firstTs?: number;
  lastTs?: number;
  approx: boolean;
}

export interface Page {
  records: DoorRecord[];
  /** Where a following read starts. Fetching never moves the cursor; acking does. */
  next: number;
  /** Where this page started reading; absent from nodes before 0.18.2. */
  start?: number;
  gap?: Gap;
}

export interface KvEntry {
  path: string;
  nodeId: string;
  topic: string;
  payload: unknown;
  ts: number;
  offset: number;
  /** Attribution of the record currently retained at this entry — the same
   *  facts `DoorRecord` carries on `/fetch`, empty when the write that
   *  produced it carried none. */
  writtenBy: string;
  actorId: string;
  actorLabel: string;
  actorKind: string;
}

export interface KvOptions {
  /** Keeps only entries of these contracts, filtered at the node. */
  contract?: string | readonly string[];
  /**
   * Keeps entries at most this many path segments below `prefix`
   * (`kv("plant/", { depth: 1 })` is the level below `plant`). The node skips
   * deeper subtrees without reading them.
   */
  depth?: number;
  signal?: AbortSignal;
}

/** One level of the tree under a prefix, as `kvLevel` reads it. */
export interface KvLevel {
  entries: KvEntry[];
  /**
   * The paths at the cut that have deeper entries, whether or not they hold a
   * record themselves: the rows a tree view can expand. Not narrowed by
   * `contract`.
   */
  folders: string[];
}

export interface SelfInfo {
  ulid: string;
  name: string;
  node: string;
  element: string;
  mount: string;
  limits: { maxRecordBytes: number; maxBlobBytes: number };
}

export interface PublishResult {
  stream: string;
  offset: number;
  topic: string;
  command?: unknown;
}

export interface FetchOptions {
  /** Records per page, 1…1000. */
  max?: number;
  /** Narrows to a subtree of the UNS path — the part after the node, not the raw topic. */
  prefix?: string;
  /** Only for the `metrics` stream, at most 1000 ids. */
  signalIds?: readonly string[];
  /** Keeps only records of these contracts; `next` still moves past the others. */
  contract?: string | readonly string[];
  /** MQTT topic filters; keeps records whose topic matches one (colca 0.19+). */
  topics?: readonly string[];
  /**
   * Read the *end* of the stream instead of the cursor's position, without
   * moving it. What a view wants when it opens: the last `max` records.
   */
  tail?: boolean;
  signal?: AbortSignal;
}

/**
 * Who a record is attributed to, when the publisher acts for someone else.
 * A service that cannot forward a person's token names their groups instead
 * and must say why — the node logs every such use.
 */
export interface Attribution {
  actorId?: string;
  actorLabel?: string;
  actorKind?: string;
  actorGroups?: readonly string[];
  fallbackReason?: string;
  writtenBy?: string;
}

export interface DoorOptions {
  /** The door's origin, e.g. `http://colca` or `https://node:443`. */
  baseUrl: string;
  /** This caller's name on the local door. Also owns the `c/{service}/…` cursors. */
  service?: string;
  /** A person's token for the published door. */
  token?: string;
  /** The node's admin token (`X-Colca-Token`), for administrative callers. */
  adminToken?: string;
  timeoutMs?: number;
  /** For tests, proxies, or a Node `Agent` that carries a client certificate. */
  fetch?: typeof globalThis.fetch;
}

/** A non-2xx answer from the door, with the node's own words kept. */
export class DoorError extends Error {
  constructor(
    readonly status: number,
    readonly route: string,
    message: string,
    /** The node's denial reason, on a 403. */
    readonly reason?: string,
  ) {
    super(`${route}: ${String(status)} ${message}`);
    this.name = "DoorError";
  }
}

export class Door {
  readonly #base: string;
  readonly #headers: Record<string, string>;
  readonly #timeout: number;
  readonly #fetch: typeof globalThis.fetch;
  readonly service?: string;

  constructor(options: DoorOptions) {
    this.#base = options.baseUrl.replace(/\/+$/, "");
    this.service = options.service;
    this.#timeout = options.timeoutMs ?? 10_000;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#headers = { "content-type": "application/json" };
    if (options.service) this.#headers["x-colca-service"] = options.service;
    if (options.token) this.#headers.authorization = `Bearer ${options.token}`;
    if (options.adminToken) this.#headers["x-colca-token"] = options.adminToken;
  }

  /** The cursor namespace this caller owns. The door refuses any other. */
  cursorName(suffix: string): string {
    if (!this.service) {
      throw new Error("cursorName() needs the service name the cursors are namespaced by");
    }
    return `c/${this.service}/${suffix}`;
  }

  /** `GET /fetch` — read forward from the cursor's stored position. */
  async fetchPage(stream: string, cursor: string, options: FetchOptions = {}): Promise<Page> {
    const query = new URLSearchParams({ stream, cursor });
    if (options.max !== undefined) query.set("max", String(options.max));
    if (options.prefix) query.set("prefix", options.prefix);
    if (options.tail) query.set("tail", "1");
    for (const id of options.signalIds ?? []) query.append("signal_id", id);
    for (const name of contractList(options.contract)) query.append("contract", name);
    for (const filter of options.topics ?? []) query.append("topic", filter);

    const body = await this.#call<WirePage>("GET", `/fetch?${query.toString()}`, undefined, options.signal);
    return {
      records: (body.records ?? []).map(toRecord),
      next: body.next,
      start: body.from,
      gap: body.gap ? toGap(body.gap) : undefined,
    };
  }

  /**
   * `POST /ack` — ack the last **processed** offset. Monotonic: the door never
   * moves a cursor backwards, and answers whether this one moved.
   */
  async ack(stream: string, cursor: string, offset: number): Promise<boolean> {
    const body = await this.#call<{ moved: boolean }>("POST", "/ack", { cursor, stream, offset });
    return body.moved;
  }

  /** `POST /ack` with `delete` — retire a cursor. Fine when it never existed. */
  async deleteCursor(stream: string, cursor: string): Promise<void> {
    await this.#call("POST", "/ack", { cursor, stream, delete: true });
  }

  /** `POST /publish` — one record under this caller's identity. */
  async publish(topic: string, payload: unknown, attribution: Attribution = {}): Promise<PublishResult> {
    return this.#call<PublishResult>("POST", "/publish", {
      topic,
      payload,
      written_by: attribution.writtenBy,
      actor_id: attribution.actorId,
      actor_label: attribution.actorLabel,
      actor_kind: attribution.actorKind,
      actor_groups: attribution.actorGroups,
      fallback_reason: attribution.fallbackReason,
    });
  }

  /** The same, with the topic built from its parts. */
  async publishTo(
    parts: TopicOptions,
    payload: unknown,
    attribution: Attribution = {},
  ): Promise<PublishResult> {
    return this.publish(buildTopic(parts), payload, attribution);
  }

  /**
   * `GET /kv` — the retained entries under `prefix`, every page followed.
   * `contract` narrows the scan at the node, before payloads are decoded;
   * `depth` stops it that many segments below `prefix`.
   */
  async kv(prefix = "", options: KvOptions = {}): Promise<KvEntry[]> {
    return (await this.#kvPages(prefix, options, false)).entries;
  }

  /**
   * `GET /kv?depth=N&folders=true` — one level of the tree under `prefix`
   * (`depth` 1 by default): its entries plus the folders that expand, so a
   * tree view never reads more than it shows.
   */
  async kvLevel(prefix = "", options: KvOptions = {}): Promise<KvLevel> {
    return this.#kvPages(prefix, { ...options, depth: options.depth ?? 1 }, true);
  }

  async #kvPages(prefix: string, options: KvOptions, folders: boolean): Promise<KvLevel> {
    const { depth } = options;
    if (depth !== undefined && (!Number.isInteger(depth) || depth < 1)) {
      throw new RangeError(`GET /kv: depth must be a positive integer, got ${String(depth)}`);
    }
    const level: KvLevel = { entries: [], folders: [] };
    let after = "";
    for (;;) {
      const query = new URLSearchParams({ prefix, max: "10000" });
      for (const name of contractList(options.contract)) query.append("contract", name);
      if (depth !== undefined) query.set("depth", String(depth));
      if (folders) query.set("folders", "true");
      if (after) query.set("after", after);

      const body = await this.#call<WireKvPage>("GET", `/kv?${query.toString()}`, undefined, options.signal);
      for (const entry of body.entries ?? []) {
        level.entries.push({
          path: entry.path,
          nodeId: entry.node_id,
          topic: entry.topic,
          payload: entry.payload,
          ts: entry.ts,
          offset: entry.offset,
          writtenBy: entry.written_by ?? "",
          actorId: entry.actor_id ?? "",
          actorLabel: entry.actor_label ?? "",
          actorKind: entry.actor_kind ?? "",
        });
      }
      level.folders.push(...(body.folders ?? []));
      const next = body.next ?? "";
      if (!next) return level;
      if (next === after) throw new Error("GET /kv: the node repeated a page token");
      after = next;
    }
  }

  /** `GET /self` — this caller's minted identity and the limits it must respect. */
  async self(): Promise<SelfInfo> {
    const body = await this.#call<WireSelf>("GET", "/self");
    return {
      ulid: body.ulid,
      name: body.name,
      node: body.node,
      element: body.element,
      mount: body.mount,
      limits: {
        maxRecordBytes: body.limits.max_record_bytes,
        maxBlobBytes: body.limits.max_blob_bytes,
      },
    };
  }

  async #call<T>(method: string, route: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    // One timeout per call, dropped again as soon as the answer is in, so a
    // long-lived client does not collect timers.
    const timer = AbortSignal.timeout(this.#timeout);
    const response = await this.#fetch(`${this.#base}${route}`, {
      method,
      headers: this.#headers,
      body: body === undefined ? undefined : JSON.stringify(stripUndefined(body)),
      signal: signal ? AbortSignal.any([signal, timer]) : timer,
    });
    const text = await response.text();
    const parsed: unknown = text ? safeJson(text) : undefined;
    if (!response.ok) {
      const error = parsed as { error?: string; reason?: string } | undefined;
      throw new DoorError(response.status, route.split("?")[0], error?.error ?? text, error?.reason);
    }
    return parsed as T;
  }
}

interface WireRecord {
  offset: number;
  origin_offset: number;
  topic: string;
  payload: unknown;
  ts: number;
  written_by?: string;
  actor_id?: string;
  actor_label?: string;
  actor_kind?: string;
}

interface WireGap {
  stream: string;
  from_offset: number;
  to_offset: number;
  first_ts?: number;
  last_ts?: number;
  approx?: boolean;
}

interface WirePage {
  records?: WireRecord[];
  next: number;
  from?: number;
  gap?: WireGap;
}

interface WireKvPage {
  entries?: {
    path: string;
    node_id: string;
    topic: string;
    payload: unknown;
    ts: number;
    offset: number;
    written_by?: string;
    actor_id?: string;
    actor_label?: string;
    actor_kind?: string;
  }[];
  folders?: string[];
  next?: string;
}

interface WireSelf {
  ulid: string;
  name: string;
  node: string;
  element: string;
  mount: string;
  limits: { max_record_bytes: number; max_blob_bytes: number };
}

function toRecord(r: WireRecord): DoorRecord {
  return {
    offset: r.offset,
    originOffset: r.origin_offset,
    topic: r.topic,
    payload: r.payload,
    ts: r.ts,
    writtenBy: r.written_by ?? "",
    actorId: r.actor_id ?? "",
    actorLabel: r.actor_label ?? "",
    actorKind: r.actor_kind ?? "",
  };
}

function toGap(g: WireGap): Gap {
  return {
    stream: g.stream,
    fromOffset: g.from_offset,
    toOffset: g.to_offset,
    firstTs: g.first_ts,
    lastTs: g.last_ts,
    approx: g.approx ?? false,
  };
}

function contractList(contract: string | readonly string[] | undefined): readonly string[] {
  return typeof contract === "string" ? [contract] : (contract ?? []);
}

/** Attribution fields left unset must not reach the node as nulls. */
function stripUndefined(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined));
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}
