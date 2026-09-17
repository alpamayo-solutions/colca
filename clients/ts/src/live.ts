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
 *   token.
 *
 * These are values, not a log: a change that happens while the connection is
 * being replaced is superseded by the retained value that follows. Anything that
 * must see every record reads a stream through the door instead.
 *
 * mqtt.js is loaded on first use and is an optional dependency of this package.
 */

import { topicMatches } from "./topics.js";

export type LiveState = "connecting" | "online" | "offline" | "closed";

export interface LiveValue<T = unknown> {
  topic: string;
  /** `undefined` when the path was retired with an empty payload. */
  payload: T | undefined;
  /** The value the node held when the subscription started, rather than a change. */
  retained: boolean;
  /** Local clock, unix milliseconds — for showing age, not for ordering records. */
  receivedAt: number;
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
    options: { qos: 0 },
    callback?: (error: Error | null, granted?: { topic: string; qos: number }[]) => void,
  ): unknown;
  unsubscribe(filters: string[]): unknown;
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

/** A token lifetime shorter than this does not make the client reconnect any faster. */
const MIN_RENEW_MS = 5_000;

export class Live {
  readonly #options: LiveOptions;
  readonly #filters = new Map<string, Set<Listener>>();
  readonly #values = new Map<string, LiveValue>();
  /** Per filter, the topics whose retained value its listeners already have on this connection. */
  readonly #retainedSent = new Map<string, Set<string>>();
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
  subscribe<T = unknown>(filter: string, listener: (value: LiveValue<T>) => void): () => void {
    if (this.#state === "closed") throw new Error("this Live client is closed");

    // A wrapper per call, so the same function subscribed twice is two subscriptions.
    const own: Listener = (value) => {
      listener(value as LiveValue<T>);
    };
    let listeners = this.#filters.get(filter);
    if (listeners === undefined) {
      listeners = new Set([own]);
      this.#filters.set(filter, listeners);
      // Offline, the filter goes out with the others once the connection is up.
      if (this.#state === "online") this.#subscribe([filter]);
    } else {
      listeners.add(own);
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
      this.#retainedSent.delete(filter);
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
    this.#values.clear();
    this.#setState("closed");
  }

  async #open(): Promise<void> {
    const generation = ++this.#generation;
    const current = (): boolean => generation === this.#generation && this.#state !== "closed";
    // A planned renewal keeps saying online; the page has nothing to show for it.
    if (this.#state !== "online") this.#setState("connecting");

    let client: MqttLike;
    let expiresAt: number | undefined;
    try {
      const token = await this.#options.token();
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
  #replace(): void {
    clearTimeout(this.#renewTimer);
    this.#renewTimer = undefined;
    this.#generation += 1;
    this.#client?.end(true);
    this.#client = undefined;
    void this.#open();
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
        this.#replace();
      },
      Math.max(MIN_RENEW_MS, due),
    );
  }

  #subscribe(filters: string[]): void {
    if (filters.length === 0) return;
    this.#client?.subscribe(filters, { qos: 0 }, (error, granted) => {
      if (error) {
        this.#report(error);
        return;
      }
      for (const grant of granted ?? []) {
        // 128 and up is a refusal: MQTT 3's failure code, or an MQTT 5 reason code.
        if (grant.qos >= 128) this.#report(new Error(`the node refused the subscription to ${grant.topic}`));
      }
    });
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
  let mqtt: typeof import("mqtt");
  try {
    mqtt = await import("mqtt");
  } catch (cause) {
    throw new MissingMqtt("colca-client/live needs the mqtt package: npm install mqtt", { cause });
  }
  return mqtt.connect(url, options);
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
