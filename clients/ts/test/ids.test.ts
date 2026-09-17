import { describe, expect, it } from "vitest";

import { newUlid } from "../src/ids.js";

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

function timeOf(ulid: string): number {
  let value = 0;
  for (const char of ulid.slice(0, 10)) value = value * 32 + CROCKFORD.indexOf(char);
  return value;
}

describe("new ULIDs", () => {
  it("carry the time they were made in, and sort by it", () => {
    const earlier = newUlid(1_789_535_138_047);
    const later = newUlid(1_789_535_138_048);

    expect(earlier).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/);
    expect(timeOf(earlier)).toBe(1_789_535_138_047);
    expect(earlier < later).toBe(true);
  });

  it("differ within the same millisecond", () => {
    const ids = new Set(Array.from({ length: 1000 }, () => newUlid(1_789_535_138_047)));
    expect(ids.size).toBe(1000);
  });

  it("refuse a time a ULID cannot hold", () => {
    expect(() => newUlid(-1)).toThrow(/2\^48/);
    expect(() => newUlid(1.5)).toThrow(/2\^48/);
  });
});
