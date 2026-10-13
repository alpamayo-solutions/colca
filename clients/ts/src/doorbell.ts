/**
 * A wake-up for a consumer that drains a stream.
 *
 * Whatever says the stream may have grown — an MQTT message on the topics the
 * consumer reads, a `/watch` hint, a reconnect — calls `ring()`. The consumer
 * takes `generation` before it drains and afterwards waits for a newer one, so
 * a ring that arrives while it drains is never lost. Rings are coalesced: many
 * rings during one drain cost one more drain.
 *
 * A ring can still go missing before it reaches the bell: a subscription the
 * broker dropped, a `/watch` connection that died without an error, a wiring
 * mistake in the caller. So a consumer bounds its wait (`timeoutMs`) and reads
 * from its cursor again when the bound passes. One lost ring then costs a
 * delay, not a consumer that stands still until it is restarted.
 */

/**
 * How long a consumer waits on a silent bell before it reads from its cursor
 * anyway: five minutes, the value the Python SDK uses. It is a recovery read
 * for a ring that never arrived, not a polling cadence — the bell is what
 * keeps a consumer current, and this must stay long enough to say so.
 */
export const IDLE_DRAIN_MS = 300_000;

/** Why a wait on the bell ended. */
export type Wake = "rang" | "timeout" | "aborted";

export interface AfterOptions {
  /** Ends the wait with `"aborted"`. */
  signal?: AbortSignal;
  /**
   * Ends the wait with `"timeout"` when nothing rang for this long. Greater
   * than zero; without it the wait has no bound.
   */
  timeoutMs?: number;
}

/** The longest delay a timer takes; a longer one would fire at once. */
const MAX_TIMER_MS = 2 ** 31 - 1;

export class Doorbell {
  #generation = 0;
  #waiters = new Set<() => void>();

  /** Increases with every ring. */
  get generation(): number {
    return this.#generation;
  }

  ring(): void {
    this.#generation += 1;
    const waiters = [...this.#waiters];
    this.#waiters.clear();
    for (const wake of waiters) wake();
  }

  /**
   * Resolves with why the wait ended: `"rang"` once the bell rang after
   * `since` was taken — at once when it already has —, `"aborted"` when
   * `signal` aborts, `"timeout"` when `timeoutMs` passed without a ring. The
   * caller reads from its cursor again on `"rang"` and on `"timeout"` alike.
   *
   * The options may also be the signal alone.
   */
  after(since: number, options: AbortSignal | AfterOptions = {}): Promise<Wake> {
    const { signal, timeoutMs }: AfterOptions = "aborted" in options ? { signal: options } : options;
    if (timeoutMs !== undefined && !(timeoutMs > 0)) {
      return Promise.reject(new RangeError(`timeoutMs must be greater than zero, got ${String(timeoutMs)}`));
    }
    if (this.#generation !== since) return Promise.resolve("rang");
    if (signal?.aborted) return Promise.resolve("aborted");

    return new Promise<Wake>((resolve) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      // Whatever ends the wait takes the other two with it, so a consumer that
      // waits for days leaves no timer and no listener behind per wait.
      const end = (why: Wake): void => {
        this.#waiters.delete(rang);
        signal?.removeEventListener("abort", aborted);
        clearTimeout(timer);
        resolve(why);
      };
      const rang = (): void => {
        end("rang");
      };
      const aborted = (): void => {
        end("aborted");
      };
      this.#waiters.add(rang);
      signal?.addEventListener("abort", aborted, { once: true });
      if (timeoutMs !== undefined) {
        timer = setTimeout(
          () => {
            end("timeout");
          },
          Math.min(timeoutMs, MAX_TIMER_MS),
        );
      }
    });
  }
}
