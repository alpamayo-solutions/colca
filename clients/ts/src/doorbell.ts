/**
 * A wake-up for a consumer that drains a stream.
 *
 * Whatever says the stream may have grown — an MQTT message on the topics the
 * consumer reads, a `/watch` hint, a reconnect — calls `ring()`. The consumer
 * takes `generation` before it drains and afterwards waits for a newer one, so
 * a ring that arrives while it drains is never lost. Rings are coalesced: many
 * rings during one drain cost one more drain.
 */
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
   * Resolves once the bell rang after `since` was taken — at once when it
   * already has — or when `signal` aborts.
   */
  async after(since: number, signal?: AbortSignal): Promise<void> {
    while (this.#generation === since && !signal?.aborted) {
      await new Promise<void>((resolve) => {
        const done = (): void => {
          this.#waiters.delete(done);
          signal?.removeEventListener("abort", done);
          resolve();
        };
        this.#waiters.add(done);
        signal?.addEventListener("abort", done, { once: true });
      });
    }
  }
}
