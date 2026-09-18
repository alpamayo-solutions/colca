/**
 * Standing alarms, and what a person does about them.
 *
 * `_AlarmState` is a retained entity: one record per alarm, under the element
 * that raised it. An alarm that goes is withdrawn with an empty payload, so
 * what is not in the node's KV is not standing — there is no `normal` record to
 * read past and no history to fold. A subscription therefore arrives with the
 * complete set and stays complete by itself.
 *
 * Quitting and silencing are `_CmdOperate` commands on the alarm's own path,
 * answered with an `_Ack`. The client never says who is quitting: the node
 * writes the session's identity onto the record, and the caller sends the note
 * and nothing else.
 *
 * The alarm's name comes from the `_SystemElement` it hangs on, and the topic
 * path is where a view jumps; neither belongs in the payload.
 */

import type { CommandAck, Live, LiveValue } from "./live.js";
import { DEFAULT_ROOT, parseTopic, topic as buildTopic } from "./topics.js";

export type AlarmSeverity = "info" | "warning" | "critical";

export type AlarmStatus = "pending" | "firing" | "unknown";

/** An `_AlarmState` record: the alarm as the node holds it while it stands. */
export interface AlarmState {
  alarm_id: string;
  status: AlarmStatus;
  severity: AlarmSeverity;
  /** Unix seconds: when it started standing. */
  since: number;
  signal_id: string;
  reason: string;
  value?: unknown;
  op?: string;
  threshold?: number;
  event_id?: string;
  /** The node's word for who quit it. A client never sends an identity. */
  acknowledged_by?: string;
  acknowledged_at?: number;
  note?: string;
  silenced_by?: string;
  /** Unix seconds: when the silence runs out. */
  silenced_until?: number;
  [field: string]: unknown;
}

/** A standing alarm and where it sits. */
export interface Alarm {
  /** Below the node: the element's path and the alarm's name. What the commands take. */
  path: string;
  topic: string;
  state: AlarmState;
}

export interface AlarmsOptions {
  live: Live;
  /** Whose alarms these are — the node level of the topic. */
  node: string;
  root?: string;
  /** How long a command waits for its `_Ack`. */
  timeoutMs?: number;
}

export interface StandingOptions {
  /** Leave out what is below this: `warning` keeps warnings and criticals. */
  minSeverity?: AlarmSeverity;
}

export interface AcknowledgeOptions {
  /** Why it was quit. The only thing the caller adds to the record. */
  note?: string;
  timeoutMs?: number;
}

export interface SilenceOptions extends AcknowledgeOptions {
  /** Unix seconds when the silence ends. Give this or `minutes`. */
  until?: number;
  minutes?: number;
}

/** The node would not do it. `result_code` reads like HTTP: 403 refused, 404 gone. */
export class AlarmRefused extends Error {
  constructor(
    readonly topic: string,
    readonly ack: CommandAck,
  ) {
    super(`${topic}: ${String(ack.result_code)} ${ack.message ?? "refused"}`);
    this.name = "AlarmRefused";
  }
}

interface Watcher {
  listener: (alarms: Alarm[]) => void;
  options: StandingOptions;
}

const RANK: Record<string, number> = { info: 1, warning: 2, critical: 3 };

export class Alarms {
  readonly #live: Live;
  readonly #node: string;
  readonly #root: string;
  readonly #timeoutMs: number;
  readonly #filter: string;
  readonly #watchers = new Set<Watcher>();
  readonly #stop: () => void;

  constructor(options: AlarmsOptions) {
    this.#live = options.live;
    this.#node = options.node;
    this.#root = options.root ?? DEFAULT_ROOT;
    this.#timeoutMs = options.timeoutMs ?? 30_000;
    // topic() refuses a wildcard segment, and rightly: the `#` goes on afterwards.
    this.#filter = `${buildTopic({ contract: "_AlarmState", node: this.#node, root: this.#root })}/#`;
    // The one subscription: it holds the set in the Live client's values, whether
    // or not anybody is watching yet.
    this.#stop = this.#live.subscribe<AlarmState>(this.#filter, () => {
      for (const watcher of this.#watchers) watcher.listener(this.standing(watcher.options));
    });
  }

  /** What stands now: worst first, and the oldest first within a severity. */
  standing(options: StandingOptions = {}): Alarm[] {
    // No floor means no floor: a severity this client does not know still shows.
    const floor = options.minSeverity === undefined ? 0 : rank(options.minSeverity);
    const alarms: Alarm[] = [];
    for (const value of this.#live.values<AlarmState>(this.#filter)) {
      const alarm = read(value);
      if (alarm !== undefined && rank(alarm.state.severity) >= floor) alarms.push(alarm);
    }
    return alarms.sort(
      (a, b) =>
        rank(b.state.severity) - rank(a.state.severity) ||
        since(a.state) - since(b.state) ||
        a.path.localeCompare(b.path),
    );
  }

  /**
   * Called with what stands now, and again on every change. Returns the function
   * that ends this subscription and no other.
   */
  onChange(listener: (alarms: Alarm[]) => void, options: StandingOptions = {}): () => void {
    const own: Watcher = { listener, options };
    this.#watchers.add(own);
    // The retained set is already in hand when a view opens late.
    queueMicrotask(() => {
      if (this.#watchers.has(own)) own.listener(this.standing(options));
    });
    return () => this.#watchers.delete(own);
  }

  /**
   * Quit a standing alarm. Throws when the node refuses and when nobody answers:
   * an acknowledgement that was swallowed is worse than one that was never sent.
   *
   * Who quit it is the node's word — only the note goes out.
   */
  acknowledge(path: string, options: AcknowledgeOptions = {}): Promise<CommandAck> {
    return this.#operate(path, "ackAlarm", note(options.note), options.timeoutMs);
  }

  /** Stop an alarm from notifying until `until` (unix seconds) or for `minutes`. */
  silence(path: string, options: SilenceOptions): Promise<CommandAck> {
    const until =
      options.until ??
      (options.minutes === undefined ? undefined : Math.round(Date.now() / 1000 + options.minutes * 60));
    if (until === undefined) {
      return Promise.reject(new Error("silence needs `until` in unix seconds or `minutes`"));
    }
    return this.#operate(path, "silenceAlarm", { until, ...note(options.note) }, options.timeoutMs);
  }

  /** Let it notify again. */
  unsilence(path: string, options: AcknowledgeOptions = {}): Promise<CommandAck> {
    return this.#operate(path, "unsilenceAlarm", note(options.note), options.timeoutMs);
  }

  /** End the subscription. The Live client goes on. */
  close(): void {
    this.#watchers.clear();
    this.#stop();
  }

  async #operate(
    path: string,
    verb: string,
    command: Record<string, unknown>,
    timeoutMs: number | undefined,
  ): Promise<CommandAck> {
    const target = buildTopic({
      contract: "_CmdOperate",
      node: this.#node,
      path: `${path}/${verb}`,
      root: this.#root,
    });
    // A verb's arguments go in `command`, which is where the Cmd contract keeps them.
    const ack = await this.#live.command(target, { command }, { timeoutMs: timeoutMs ?? this.#timeoutMs });
    if (ack.result_code >= 300) throw new AlarmRefused(target, ack);
    return ack;
  }
}

/** A cached value as an alarm, or nothing when it is neither. */
function read(value: LiveValue<AlarmState>): Alarm | undefined {
  const parts = parseTopic(value.topic);
  if (parts === undefined || parts.path === "" || value.payload === undefined) return undefined;
  return { path: parts.path, topic: value.topic, state: value.payload };
}

function rank(severity: string): number {
  return RANK[severity] ?? 0;
}

function since(state: AlarmState): number {
  return Number.isFinite(state.since) ? state.since : 0;
}

function note(text: string | undefined): Record<string, unknown> {
  return text === undefined ? {} : { note: text };
}
