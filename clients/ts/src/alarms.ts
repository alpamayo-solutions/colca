/**
 * Standing alarms, and what a person does about them.
 *
 * `_AlarmState` is a retained entity: one record per alarm, under the element
 * that raised it. An alarm that goes is withdrawn with an empty payload, so
 * what is not in the node's KV is not standing — there is no `normal` record to
 * read past and no history to fold. A subscription therefore arrives with the
 * complete set. The one thing it cannot see by itself is an alarm that went
 * while the client hung between two connections: nothing is left to deliver for
 * it. `#resync` is what settles that.
 *
 * Quitting is a `_CmdAcknowledge` and silencing an `_CmdOperate`, each on the
 * alarm's own path and answered with an `_Ack`. They are two contracts so that
 * a grant for `acknowledge` lets someone quit an alarm without being allowed to
 * operate the line, while silencing, which keeps a notification from other
 * people, stays with `operate`. The client never says who is quitting: the node
 * writes the session's identity onto the record, and the caller sends the note
 * and nothing else.
 *
 * The alarm's name comes from the `_SystemElement` it hangs on, and the topic
 * path is where a view jumps; neither belongs in the payload.
 */

import type { AlarmState } from "./generated/contracts.js";
import type { CommandAck, Live, LiveValue } from "./live.js";
import { DEFAULT_ROOT, parseTopic, topic as buildTopic } from "./topics.js";

// The record itself comes from the contract bundle, and the two vocabularies
// with it, so a status the node stops accepting stops compiling here.
export type { AlarmState };

export type AlarmSeverity = AlarmState["severity"];

export type AlarmStatus = AlarmState["status"];

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
  /**
   * How long the retained set has to arrive again after a reconnection before
   * what did not arrive counts as gone. 0 leaves the view as it stood.
   */
  resyncMs?: number;
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
  readonly #resyncMs: number;
  readonly #filter: string;
  readonly #standing = new Map<string, Alarm>();
  readonly #watchers = new Set<Watcher>();
  readonly #stop: () => void;
  readonly #stopResync: () => void;
  /** The topics the node has repeated since the last resubscription, while a window is open. */
  #seen: Set<string> | undefined;
  #resyncTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: AlarmsOptions) {
    this.#live = options.live;
    this.#node = options.node;
    this.#root = options.root ?? DEFAULT_ROOT;
    this.#timeoutMs = options.timeoutMs ?? 30_000;
    this.#resyncMs = options.resyncMs ?? 750;
    // topic() refuses a wildcard segment, and rightly: the `#` goes on afterwards.
    this.#filter = `${buildTopic({ contract: "_AlarmState", node: this.#node, root: this.#root })}/#`;
    this.#stop = this.#live.subscribe<AlarmState>(this.#filter, (value) => {
      this.#receive(value);
    });
    this.#stopResync = this.#live.onResubscribe(() => {
      this.#resync();
    });
  }

  /** What stands now: worst first, and the oldest first within a severity. */
  standing(options: StandingOptions = {}): Alarm[] {
    // No floor means no floor: a severity this client does not know still shows.
    const floor = options.minSeverity === undefined ? 0 : rank(options.minSeverity);
    const alarms = [...this.#standing.values()].filter((alarm) => rank(alarm.state.severity) >= floor);
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
    return this.#command("_CmdAcknowledge", path, "ackAlarm", note(options.note), options.timeoutMs);
  }

  /** Put a read notice back to unread. The manager refuses it for anything above `info`. */
  unacknowledge(path: string, options: Omit<AcknowledgeOptions, "note"> = {}): Promise<CommandAck> {
    return this.#command("_CmdAcknowledge", path, "unackAlarm", {}, options.timeoutMs);
  }

  /** Stop an alarm from notifying until `until` (unix seconds) or for `minutes`. */
  silence(path: string, options: SilenceOptions): Promise<CommandAck> {
    const until =
      options.until ??
      (options.minutes === undefined ? undefined : Math.round(Date.now() / 1000 + options.minutes * 60));
    if (until === undefined) {
      return Promise.reject(new Error("silence needs `until` in unix seconds or `minutes`"));
    }
    return this.#command(
      "_CmdOperate",
      path,
      "silenceAlarm",
      { until, ...note(options.note) },
      options.timeoutMs,
    );
  }

  /** Let it notify again. */
  unsilence(path: string, options: AcknowledgeOptions = {}): Promise<CommandAck> {
    return this.#command("_CmdOperate", path, "unsilenceAlarm", note(options.note), options.timeoutMs);
  }

  /** End the subscription. The Live client goes on. */
  close(): void {
    clearTimeout(this.#resyncTimer);
    this.#resyncTimer = undefined;
    this.#seen = undefined;
    this.#watchers.clear();
    this.#standing.clear();
    this.#stopResync();
    this.#stop();
  }

  #receive(value: LiveValue<AlarmState>): void {
    const parts = parseTopic(value.topic);
    if (parts === undefined || parts.path === "") return;
    this.#seen?.add(value.topic);
    // An empty payload is the alarm going: what is not in the node's KV is not standing.
    if (value.payload === undefined) this.#standing.delete(value.topic);
    else this.#standing.set(value.topic, { path: parts.path, topic: value.topic, state: value.payload });
    this.#announce();
  }

  /**
   * A new connection starts the node's retained delivery over, and an alarm that
   * went while the client was away leaves nothing behind to say so: its record
   * was withdrawn at the broker, so nothing arrives for it ever again. What the
   * node does not repeat within the window is therefore taken as gone.
   *
   * It is a window, and a guess, because the node does not say where its retained
   * delivery ends. It runs from the resubscription rather than from the first
   * record, since an empty set sends nothing at all.
   */
  #resync(): void {
    if (this.#resyncMs === 0) return;
    clearTimeout(this.#resyncTimer);
    this.#seen = new Set();
    this.#resyncTimer = setTimeout(() => {
      const repeated = this.#seen ?? new Set<string>();
      this.#seen = undefined;
      this.#resyncTimer = undefined;
      let gone = false;
      for (const topic of [...this.#standing.keys()]) {
        if (repeated.has(topic)) continue;
        this.#standing.delete(topic);
        gone = true;
      }
      // Silence when nothing went: a reconnection on its own is not a change.
      if (gone) this.#announce();
    }, this.#resyncMs);
  }

  /** Every watcher hears it, and one that throws does not silence the others. */
  #announce(): void {
    let failure: Error | undefined;
    for (const watcher of this.#watchers) {
      try {
        watcher.listener(this.standing(watcher.options));
      } catch (error) {
        failure ??= error instanceof Error ? error : new Error(String(error));
      }
    }
    if (failure !== undefined) throw failure;
  }

  async #command(
    contract: "_CmdAcknowledge" | "_CmdOperate",
    path: string,
    verb: string,
    command: Record<string, unknown>,
    timeoutMs: number | undefined,
  ): Promise<CommandAck> {
    const target = buildTopic({
      contract,
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

function rank(severity: string): number {
  return RANK[severity] ?? 0;
}

function since(state: AlarmState): number {
  return Number.isFinite(state.since) ? state.since : 0;
}

function note(text: string | undefined): Record<string, unknown> {
  return text === undefined ? {} : { note: text };
}
