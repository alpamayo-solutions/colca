/**
 * Live values over MQTT, for a page or a service that wants them pushed.
 *
 * Colca retains the current value of every data and entity path, so a
 * subscription brings the present state first and the changes after it. There
 * is no snapshot to fetch and nothing to merge. What this module adds is what a
 * long-lived client needs around that:
 *
 * - one connection, however many parts of a page subscribe, and an unsubscribe
 *   that ends only its own subscription;
 * - the last value of every topic, so a second subscriber to a topic gets it at
 *   once instead of waiting for the next change;
 * - a fresh token before the old one runs out — the node ends a session when
 *   its token expires — and every subscription sent again on the new connection;
 * - growing, jittered waits between attempts after a drop, each with a fresh
 *   token;
 * - commands that wait for their acknowledgement.
 *
 * These are values, not a log: a change that happens while the connection is
 * being replaced is superseded by the retained value that follows. Anything that
 * must see every record reads a stream through the door instead.
 *
 * mqtt.js is loaded on first use and is an optional dependency of this package.
 */

import { newUlid } from "./ids.js";
import { parseTopic, topicMatches } from "./topics.js";

export type LiveState = "connecting" | "online" | "offline" | "closed";

export type Qos = 0 | 1;

export interface LiveValue<T = unknown> {
  topic: string;
  /** `undefined` when the path was retired with an empty payload. */
  payload: T | undefined;
  /** The value the node held when the subscription started, rather than a change. */
  retained: boolean;
  /** Local clock, unix milliseconds — for showing age, not for ordering records. */
  receivedAt: number;
}

export interface SubscribeOptions {
  /** 1 for records that must not get lost in flight, acknowledgements for instance. */
  qos?: Qos;
}

/** A command's answer, `_Ack`. `result_code` reads like HTTP: 200 means done. */
export interface CommandAck {
  correlation_id: string;
  result_code: number;
  message?: string;
  performed_at?: number | null;
  [field: string]: unknown;
}

export interface CommandOptions {
  /** How long the command stays valid at the node, and how long to wait for its answer. */
  timeoutMs?: number;
  /** Where the answer arrives: every `_Ack` under the command's root, unless given. */
  ackFilter?: string;
}

/** Nobody answered a command before it expired. */
export class CommandTimeout extends Error {
  constructor(
    readonly topic: string,
    readonly correlationId: string,
  ) {
    super(`nobody acknowledged ${topic} (${correlationId}) before it expired`);
    this.name = "CommandTimeout";
  }
}

/** What is handed to the MQTT client for each connection. */
export interface MqttConnectOptions {
  clientId: string;
  username: string;
  password: string;
  protocolVersion: 5;
  clean: true;
  keepalive: number;
  /** Zero: reconnecting is this module's job, because every attempt needs a fresh token. */
  reconnectPeriod: 0;
  connectTimeout: number;
  [option: string]: unknown;
}

/** The part of an MQTT client this module uses; mqtt.js's client fits. */
export interface MqttLike {
  on(event: "connect" | "close", listener: () => void): unknown;
  on(event: "error", listener: (error: Error) => void): unknown;
  on(
    event: "message",
    listener: (topic: string, payload: Uint8Array, packet: { retain: boolean }) => void,
  ): unknown;
  subscribe(
    filters: string[],
    options: { qos: Qos },
    callback?: (error: Error | null, granted?: { topic: string; qos: number }[]) => void,
  ): unknown;
  unsubscribe(filters: string[]): unknown;
  publish(
    topic: string,
    message: string,
    options: { qos: Qos; retain: false },
    callback?: (error?: Error) => void,
  ): unknown;
  end(force?: boolean): unknown;
}

export interface LiveOptions {
  /** The node's WebSocket door for people and applications, e.g. `wss://node:8885`. */
  url: string;
  /** Asked before every connection attempt; hand back a token that is valid now. */
  token: () => string | Promise<string>;
  /** The node requires the token's `sub` here, which is the default. Set it for a token that is not a JWT. */
  username?: string;
  /** Reconnect with a new token this long before the current one expires. */
  renewBeforeMs?: number;
  /** The first wait after a failed attempt. It doubles up to `maxRetryMs`. */
  retryMs?: number;
  maxRetryMs?: number;
  clientIdPrefix?: string;
  /** Passed to the MQTT client as they are — TLS settings for a service, for instance. */
  mqttOptions?: Readonly<Record<string, unknown>>;
  /** Problems the client lives through: refused connections, unreadable payloads, throwing listeners. */
  onError?: (error: Error) => void;
  /** How a connection is made. mqtt.js by default. */
  connect?: (url: string, options: MqttConnectOptions) => MqttLike | Promise<MqttLike>;
}

type Listener = (value: LiveValue) => void;

interface Pending {
  resolve: (ack: CommandAck) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

/** A token lifetime shorter than this does not make the client reconnect any faster. */
const MIN_RENEW_MS = 5_000;

export class Live {
  readonly #options: LiveOptions;
  readonly #filters = new Map<string, Set<Listener>>();
  readonly #qos = new Map<string, Qos>();
  readonly #values = new Map<string, LiveValue>();
  /** Per filter, the topics whose retained value its listeners already have on this connection. */
  readonly #retainedSent = new Map<string, Set<string>>();
  /** Filters the node has confirmed on this connection, and who waits for that. */
  readonly #granted = new Set<string>();
  readonly #grantWaiters = new Map<string, (() => void)[]>();
  readonly #ackFilters = new Set<string>();
  readonly #pending = new Map<string, Pending>();
  readonly #stateListeners = new Set<(state: LiveState) => void>();
  #state: LiveState = "connecting";
  #client: MqttLike | undefined;
  #generation = 0;
  #attempt = 0;
  #renewTimer: ReturnType<typeof setTimeout> | undefined;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: LiveOptions) {
    this.#options = options;
    void this.#open();
  }

  get state(): LiveState {
    return this.#state;
  }

  /**
   * Values under `filter` (MQTT wildcards allowed), the current ones first. Returns
   * the function that ends this subscription and no other.
   */
  subscribe<T = unknown>(
    filter: string,
    listener: (value: LiveValue<T>) => void,
    options: SubscribeOptions = {},
  ): () => void {
    if (this.#state === "closed") throw new Error("this Live client is closed");
    const qos = options.qos ?? 0;

    // A wrapper per call, so the same function subscribed twice is two subscriptions.
    const own: Listener = (value) => {
      listener(value as LiveValue<T>);
    };
    let listeners = this.#filters.get(filter);
    if (listeners === undefined) {
      listeners = new Set([own]);
      this.#filters.set(filter, listeners);
      this.#qos.set(filter, qos);
      // Offline, the filter goes out with the others once the connection is up.
      if (this.#state === "online") this.#subscribe([filter]);
    } else {
      listeners.add(own);
      if (qos > (this.#qos.get(filter) ?? 0)) {
        // Subscribing again replaces the node's subscription with the stronger one.
        this.#qos.set(filter, qos);
        if (this.#state === "online") this.#subscribe([filter]);
      }
      // The node sends retained values once per subscription, and this filter's
      // went to whoever subscribed first. The newcomer gets them from here.
      const current = this.values(filter);
      queueMicrotask(() => {
        if (!this.#filters.get(filter)?.has(own)) return;
        for (const value of current) this.#deliver(own, value);
      });
    }

    return () => {
      const set = this.#filters.get(filter);
      if (!set?.delete(own) || set.size > 0) return;
      this.#filters.delete(filter);
      this.#qos.delete(filter);
      this.#retainedSent.delete(filter);
      this.#granted.delete(filter);
      if (this.#state === "online") this.#client?.unsubscribe([filter]);
      this.#forget(filter);
    };
  }

  /** The last value seen on a topic, if anyone is subscribed to it. */
  latest<T = unknown>(topic: string): LiveValue<T> | undefined {
    return this.#values.get(topic) as LiveValue<T> | undefined;
  }

  /** Every last value under a filter. */
  values<T = unknown>(filter: string): LiveValue<T>[] {
    return [...this.#values.values()].filter((value) => topicMatches(filter, value.topic)) as LiveValue<T>[];
  }

  /**
   * One record, as JSON, under this caller's identity. Nothing is queued: offline
   * it fails at once, because a command sent minutes later is a different command.
   * A person's session may only send commands; the node refuses anything else.
   */
  publish(topic: string, payload: unknown, options: { qos?: Qos } = {}): Promise<void> {
    const client = this.#client;
    if (this.#state !== "online" || client === undefined) {
      return Promise.reject(new Error(`not connected: ${topic} was not sent`));
    }
    return new Promise((resolve, reject) => {
      client.publish(topic, JSON.stringify(payload), { qos: options.qos ?? 1, retain: false }, (error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  /**
   * Send a command and wait for its acknowledgement.
   *
   * `body` is the command's own fields — `params` for a `_CmdParam`, say. The
   * correlation id and the expiry are added here, and the answer is matched by
   * the id, wherever in the tree the executor publishes it. The promise settles
   * with the `_Ack`, whatever its `result_code`; it rejects when nothing answers
   * in `timeoutMs`, when the node refuses the publish, or when the client closes.
   */
  command(
    topic: string,
    body: Record<string, unknown> = {},
    options: CommandOptions = {},
  ): Promise<CommandAck> {
    const parts = parseTopic(topic);
    if (!parts?.contract.startsWith("_Cmd")) {
      return Promise.reject(new Error(`a command goes to a _Cmd… topic, got ${topic}`));
    }
    const timeoutMs = options.timeoutMs ?? 30_000;
    const ackFilter = options.ackFilter ?? `${parts.root}/${parts.version}/_Ack/#`;
    const correlationId = typeof body.correlation_id === "string" ? body.correlation_id : newUlid();
    this.#listenForAcks(ackFilter);

    return new Promise<CommandAck>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.#pending.delete(correlationId)) reject(new CommandTimeout(topic, correlationId));
      }, timeoutMs);
      this.#pending.set(correlationId, { resolve, reject, timer });

      // Only once the node confirmed the subscription to the answers: an executor
      // that answers at once must not answer into nothing.
      this.#whenGranted(ackFilter)
        .then(() =>
          this.publish(topic, { ...body, correlation_id: correlationId, expires_at: Date.now() + timeoutMs }),
        )
        .catch((error: unknown) => {
          const pending = this.#pending.get(correlationId);
          if (!pending) return;
          this.#pending.delete(correlationId);
          clearTimeout(pending.timer);
          pending.reject(error instanceof Error ? error : new Error(String(error)));
        });
    });
  }

  /** Called on every change of the connection state; returns the function that stops it. */
  onState(listener: (state: LiveState) => void): () => void {
    this.#stateListeners.add(listener);
    return () => this.#stateListeners.delete(listener);
  }

  close(): void {
    if (this.#state === "closed") return;
    this.#generation += 1;
    clearTimeout(this.#renewTimer);
    clearTimeout(this.#retryTimer);
    this.#client?.end();
    this.#client = undefined;
    this.#filters.clear();
    this.#qos.clear();
    this.#values.clear();
    this.#granted.clear();
    this.#grantWaiters.clear();
    this.#ackFilters.clear();
    for (const [id, pending] of this.#pending) {
      clearTimeout(pending.timer);
      pending.reject(new Error(`this Live client closed before ${id} was acknowledged`));
    }
    this.#pending.clear();
    this.#setState("closed");
  }

  /** Connect, with `token` when the renewal already fetched one. */
  async #open(token?: string): Promise<void> {
    const generation = ++this.#generation;
    const current = (): boolean => generation === this.#generation && this.#state !== "closed";
    // A planned renewal keeps saying online; the page has nothing to show for it.
    if (this.#state !== "online") this.#setState("connecting");

    let client: MqttLike;
    let expiresAt: number | undefined;
    try {
      token ??= await this.#options.token();
      const claims = readClaims(token);
      const username = this.#options.username ?? claims?.sub;
      if (!username) {
        throw new Error("the token carries no sub: pass `username` for a token that is not a JWT");
      }
      expiresAt = claims?.exp === undefined ? undefined : claims.exp * 1000;
      const connect = this.#options.connect ?? connectWithMqttJs;
      client = await connect(this.#options.url, {
        ...this.#options.mqttOptions,
        clientId: `${this.#options.clientIdPrefix ?? "colca-client"}-${crypto.randomUUID()}`,
        username,
        password: token,
        protocolVersion: 5,
        clean: true,
        keepalive: 30,
        reconnectPeriod: 0,
        connectTimeout: 10_000,
      });
    } catch (error) {
      if (!current()) return;
      this.#report(error);
      if (error instanceof MissingMqtt) {
        this.close();
        return;
      }
      this.#setState("offline");
      this.#retry();
      return;
    }

    if (!current()) {
      client.end(true);
      return;
    }
    this.#client = client;

    client.on("connect", () => {
      if (!current()) return;
      this.#attempt = 0;
      this.#setState("online");
      this.#retainedSent.clear();
      this.#granted.clear();
      this.#subscribe([...this.#filters.keys()]);
      this.#scheduleRenewal(expiresAt);
    });
    client.on("message", (topic, payload, packet) => {
      if (current()) this.#receive(topic, payload, packet.retain);
    });
    client.on("error", (error) => {
      if (current()) this.#report(error);
    });
    client.on("close", () => {
      if (!current()) return;
      this.#setState("offline");
      this.#retry();
    });
  }

  /** End the connection in hand and open the next one. */
  #replace(token?: string): void {
    clearTimeout(this.#renewTimer);
    this.#renewTimer = undefined;
    this.#generation += 1;
    this.#client?.end(true);
    this.#client = undefined;
    void this.#open(token);
  }

  #retry(): void {
    if (this.#retryTimer !== undefined) return;
    const ceiling = Math.min(
      this.#options.maxRetryMs ?? 30_000,
      (this.#options.retryMs ?? 1_000) * 2 ** this.#attempt,
    );
    this.#attempt += 1;
    // Full jitter: a hundred pages that lost the same node do not come back in step.
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      this.#replace();
    }, Math.random() * ceiling);
  }

  #scheduleRenewal(expiresAt: number | undefined): void {
    clearTimeout(this.#renewTimer);
    // A token without an expiry, such as a personal access token, is not renewed on a clock.
    if (expiresAt === undefined) return;
    const due = expiresAt - (this.#options.renewBeforeMs ?? 30_000) - Date.now();
    this.#renewTimer = setTimeout(
      () => {
        this.#renewTimer = undefined;
        void this.#renew(expiresAt);
      },
      Math.max(MIN_RENEW_MS, due),
    );
  }

  /**
   * Replace the connection only with a token that outlives it. An identity proxy
   * can keep handing out the token it holds until its own refresh is due, and
   * reconnecting with that one gains nothing and costs a gap in the values.
   */
  async #renew(expiresAt: number): Promise<void> {
    const generation = this.#generation;
    let token: string | undefined;
    try {
      token = await this.#options.token();
    } catch (error) {
      if (generation === this.#generation) this.#report(error);
    }
    if (generation !== this.#generation || this.#state === "closed") return;

    const exp = token === undefined ? undefined : readClaims(token)?.exp;
    if (token !== undefined && (exp === undefined || exp * 1000 > expiresAt)) {
      this.#replace(token);
      return;
    }
    // Ask again shortly; once the token has run out, the node ends the session and
    // reconnecting takes over.
    if (Date.now() + MIN_RENEW_MS >= expiresAt) return;
    this.#renewTimer = setTimeout(() => {
      this.#renewTimer = undefined;
      void this.#renew(expiresAt);
    }, MIN_RENEW_MS);
  }

  #subscribe(filters: string[]): void {
    const generation = this.#generation;
    for (const qos of [0, 1] as const) {
      const group = filters.filter((filter) => (this.#qos.get(filter) ?? 0) === qos);
      if (group.length === 0) continue;
      this.#client?.subscribe(group, { qos }, (error, granted) => {
        if (generation !== this.#generation) return;
        if (error) {
          this.#report(error);
          return;
        }
        for (const grant of granted ?? []) {
          // 128 and up is a refusal: MQTT 3's failure code, or an MQTT 5 reason code.
          if (grant.qos >= 128) {
            this.#report(new Error(`the node refused the subscription to ${grant.topic}`));
            continue;
          }
          this.#granted.add(grant.topic);
          for (const wake of this.#grantWaiters.get(grant.topic) ?? []) wake();
          this.#grantWaiters.delete(grant.topic);
        }
      });
    }
  }

  #whenGranted(filter: string): Promise<void> {
    if (this.#granted.has(filter)) return Promise.resolve();
    return new Promise((resolve) => {
      this.#grantWaiters.set(filter, [...(this.#grantWaiters.get(filter) ?? []), resolve]);
    });
  }

  /** One subscription per acknowledgement filter, kept for as long as the client lives. */
  #listenForAcks(filter: string): void {
    if (this.#ackFilters.has(filter)) return;
    this.#ackFilters.add(filter);
    this.subscribe(
      filter,
      (value) => {
        const ack = value.payload;
        if (typeof ack !== "object" || ack === null) return;
        const id = (ack as { correlation_id?: unknown }).correlation_id;
        const pending = typeof id === "string" ? this.#pending.get(id) : undefined;
        // Somebody else's command, or one that already timed out.
        if (pending === undefined || typeof id !== "string") return;
        this.#pending.delete(id);
        clearTimeout(pending.timer);
        pending.resolve(ack as CommandAck);
      },
      { qos: 1 },
    );
  }

  #receive(topic: string, payload: Uint8Array, retained: boolean): void {
    let decoded: unknown;
    if (payload.length > 0) {
      try {
        decoded = JSON.parse(new TextDecoder().decode(payload));
      } catch {
        this.#report(new Error(`unreadable payload on ${topic}`));
        return;
      }
    }

    const value: LiveValue = { topic, payload: decoded, retained, receivedAt: Date.now() };
    if (decoded === undefined) this.#values.delete(topic);
    else this.#values.set(topic, value);

    for (const [filter, listeners] of this.#filters) {
      if (!topicMatches(filter, topic)) continue;
      if (retained && !this.#firstRetained(filter, topic)) continue;
      for (const listener of listeners) this.#deliver(listener, value);
    }
  }

  /**
   * The node answers every subscription with the retained values under it, so a
   * topic covered by two filters arrives retained twice when the second filter is
   * subscribed. Each filter's listeners get it once per connection.
   */
  #firstRetained(filter: string, topic: string): boolean {
    let sent = this.#retainedSent.get(filter);
    if (sent === undefined) {
      sent = new Set();
      this.#retainedSent.set(filter, sent);
    }
    if (sent.has(topic)) return false;
    sent.add(topic);
    return true;
  }

  #deliver(listener: Listener, value: LiveValue): void {
    try {
      listener(value);
    } catch (error) {
      this.#report(error);
    }
  }

  /** Drop the values no remaining filter covers, so a later subscriber is not handed stale ones. */
  #forget(filter: string): void {
    for (const topic of this.#values.keys()) {
      if (!topicMatches(filter, topic)) continue;
      if (![...this.#filters.keys()].some((other) => topicMatches(other, topic))) this.#values.delete(topic);
    }
  }

  #setState(state: LiveState): void {
    if (state === this.#state) return;
    this.#state = state;
    for (const listener of this.#stateListeners) listener(state);
  }

  #report(error: unknown): void {
    this.#options.onError?.(error instanceof Error ? error : new Error(String(error)));
  }
}

class MissingMqtt extends Error {}

async function connectWithMqttJs(url: string, options: MqttConnectOptions): Promise<MqttLike> {
  let mqtt: Partial<typeof import("mqtt")>;
  try {
    mqtt = await import("mqtt");
  } catch (cause) {
    throw new MissingMqtt("colca-client/live needs the mqtt package: npm install mqtt", { cause });
  }
  // A bundler hands a browser mqtt.js's ESM build, which exports only a default;
  // Node gets the CommonJS build, where connect is also a named export.
  const connect = mqtt.connect ?? mqtt.default?.connect;
  if (connect === undefined) throw new MissingMqtt("the mqtt package in use has no connect()");
  return connect(url, options);
}

/** `sub` and `exp` from a JWT, without checking it — the node does that. */
function readClaims(token: string): { sub?: string; exp?: number } | undefined {
  const parts = token.split(".");
  if (parts.length !== 3) return undefined;
  try {
    const base64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const bytes = Uint8Array.from(atob(base64), (char) => char.charCodeAt(0));
    const claims = JSON.parse(new TextDecoder().decode(bytes)) as { sub?: unknown; exp?: unknown };
    return {
      sub: typeof claims.sub === "string" ? claims.sub : undefined,
      exp: typeof claims.exp === "number" ? claims.exp : undefined,
    };
  } catch {
    return undefined;
  }
}
