import { vi } from "vitest";

import { Live, type LiveOptions, type MqttConnectOptions } from "../src/live.js";

type Handler = (...args: unknown[]) => void;

/** Stands in for mqtt.js: records what it is asked and lets a test play the broker. */
export class FakeClient {
  readonly subscribed: string[] = [];
  readonly subscribedQos: [string, number][] = [];
  readonly unsubscribed: string[] = [];
  readonly sent: { topic: string; body: Record<string, unknown>; qos: number }[] = [];
  /** Set to hold SUBACKs back until `grant()`. */
  holdSubacks = false;
  /** Set to make the node refuse publishes. */
  refusePublish: Error | undefined;
  ended = false;
  /** How it was ended: true drops the connection, false sends a DISCONNECT first. */
  endedForce: boolean | undefined;
  /** Set to play a node that renews tokens in place. */
  reauthenticate?: (token: string) => Promise<void>;
  readonly #handlers = new Map<string, Handler[]>();
  readonly #held: (() => void)[] = [];

  constructor(readonly options: MqttConnectOptions) {}

  on(event: string, handler: Handler): this {
    this.#handlers.set(event, [...(this.#handlers.get(event) ?? []), handler]);
    return this;
  }

  subscribe(
    filters: string[],
    options: { qos: number },
    callback?: (error: Error | null, granted: { topic: string; qos: number }[]) => void,
  ): void {
    this.subscribed.push(...filters);
    for (const filter of filters) this.subscribedQos.push([filter, options.qos]);
    const answer = (): void =>
      callback?.(
        null,
        filters.map((topic) => ({ topic, qos: options.qos })),
      );
    if (this.holdSubacks) this.#held.push(answer);
    else answer();
  }

  grant(): void {
    for (const answer of this.#held.splice(0)) answer();
  }

  unsubscribe(filters: string[]): void {
    this.unsubscribed.push(...filters);
  }

  publish(
    topic: string,
    message: string,
    options: { qos: number },
    callback?: (error?: Error) => void,
  ): void {
    this.sent.push({ topic, body: JSON.parse(message) as Record<string, unknown>, qos: options.qos });
    callback?.(this.refusePublish);
  }

  end(force = false): void {
    this.ended = true;
    this.endedForce = force;
  }

  emit(event: string, ...args: unknown[]): void {
    for (const handler of this.#handlers.get(event) ?? []) handler(...args);
  }

  /** The broker delivering a message to this client. */
  deliver(topic: string, payload: unknown, retain = false): void {
    const bytes =
      payload === undefined ? new Uint8Array() : new TextEncoder().encode(JSON.stringify(payload));
    this.emit("message", topic, bytes, { retain });
  }
}

export function jwt(claims: Record<string, unknown>): string {
  const part = (value: unknown): string => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${part({ alg: "none" })}.${part(claims)}.signature`;
}

export function setup(options: Partial<LiveOptions> = {}): {
  live: Live;
  clients: FakeClient[];
  tokens: string[];
  errors: Error[];
} {
  const clients: FakeClient[] = [];
  const tokens: string[] = [];
  const errors: Error[] = [];
  const live = new Live({
    url: "wss://node:8885",
    token: () => {
      const token = jwt({ sub: "till", exp: Math.floor(Date.now() / 1000) + 300, n: tokens.length });
      tokens.push(token);
      return token;
    },
    connect: (_url, connectOptions) => {
      const client = new FakeClient(connectOptions);
      clients.push(client);
      return client as never;
    },
    onError: (error) => errors.push(error),
    ...options,
  });
  return { live, clients, tokens, errors };
}

/** Let the token and connect promises settle. */
export const settle = (): Promise<void> => vi.advanceTimersByTimeAsync(0).then(() => undefined);
