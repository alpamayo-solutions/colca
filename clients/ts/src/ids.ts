/**
 * Derived record ids.
 *
 * An annotation's id is computed, not drawn: create, update and delete of one
 * annotation are appends under the same id, so a producer that runs again
 * overwrites its own record instead of doubling it. Every client must therefore
 * compute it exactly as `colca-data-contracts` does — `spec/vectors.json` holds
 * the cases that prove it, including the one where the microseconds decide.
 */

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

export interface AnnotationIdInput {
  annotationTypeId: string;
  source: string;
  /** Unix seconds. Six decimals are part of the identity; the seventh is not. */
  timeStart: number;
  /** The set is sorted before hashing, so the order it arrives in does not matter. */
  signalIds: readonly string[];
}

/**
 * `SHA-256("{type}|{source}|{time:.6f}|{sorted,signals}")`, first 16 bytes,
 * ULID-encoded.
 *
 * Async because it hashes with WebCrypto, the one digest both Node and a
 * browser have without a dependency.
 */
export async function deriveAnnotationId(input: AnnotationIdInput): Promise<string> {
  const material = annotationIdMaterial(input);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(material));
  return ulidFromBytes(new Uint8Array(digest, 0, 16));
}

/** The string that gets hashed. Exported for the conformance test's error messages. */
export function annotationIdMaterial({
  annotationTypeId,
  source,
  timeStart,
  signalIds,
}: AnnotationIdInput): string {
  if (!Number.isFinite(timeStart)) {
    throw new Error(`timeStart must be a finite unix time, got ${String(timeStart)}`);
  }
  const signals = [...signalIds].sort().join(",");
  return `${annotationTypeId}|${source}|${timeStart.toFixed(6)}|${signals}`;
}

/**
 * A new ULID: 48 bits of unix milliseconds, then 80 random bits. Ids made later
 * sort later, which is the shape the node's own ids have — use it for command ids
 * and correlation ids.
 */
export function newUlid(now: number = Date.now()): string {
  if (!Number.isInteger(now) || now < 0 || now >= 2 ** 48) {
    throw new Error(`a ULID time is whole milliseconds between 0 and 2^48, got ${String(now)}`);
  }
  const bytes = new Uint8Array(16);
  let time = now;
  for (let i = 5; i >= 0; i -= 1) {
    bytes[i] = time % 256;
    time = Math.floor(time / 256);
  }
  crypto.getRandomValues(bytes.subarray(6));
  return ulidFromBytes(bytes);
}

/** Crockford base32 over 128 bits, the 26-character ULID text. */
export function ulidFromBytes(bytes: Uint8Array): string {
  if (bytes.length !== 16) {
    throw new Error(`a ULID is 16 bytes, got ${String(bytes.length)}`);
  }
  let value = 0n;
  for (const byte of bytes) value = (value << 8n) | BigInt(byte);
  let out = "";
  for (let i = 0; i < 26; i += 1) {
    out = CROCKFORD[Number(value & 31n)] + out;
    value >>= 5n;
  }
  return out;
}
