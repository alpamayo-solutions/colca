/**
 * The vectors in `clients/spec/vectors.json` come out of
 * `colca-data-contracts`, the package the node validates against. If a rule
 * changes there, these tests fail here — which is the whole point of them.
 *
 * Regenerate with `uv run --project contracts python clients/spec/generate_vectors.py`.
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { annotationIdMaterial, deriveAnnotationId } from "../src/ids.js";
import { topic } from "../src/topics.js";

interface Vectors {
  annotation_id: {
    note: string;
    annotation_type_id: string;
    source: string;
    time_start: number;
    signal_ids: string[];
    expected: string;
  }[];
  topic: { note: string; root: string; contract: string; node: string; path: string; expected: string }[];
}

const vectors = JSON.parse(
  readFileSync(fileURLToPath(new URL("../../spec/vectors.json", import.meta.url)), "utf8"),
) as Vectors;

describe("annotation ids", () => {
  it.each(vectors.annotation_id)("$note", async (vector) => {
    const id = await deriveAnnotationId({
      annotationTypeId: vector.annotation_type_id,
      source: vector.source,
      timeStart: vector.time_start,
      signalIds: vector.signal_ids,
    });
    // The hashed string is in the message, because a mismatch is almost always
    // the formatting of the seconds rather than the hash.
    expect(
      id,
      annotationIdMaterial({
        annotationTypeId: vector.annotation_type_id,
        source: vector.source,
        timeStart: vector.time_start,
        signalIds: vector.signal_ids,
      }),
    ).toBe(vector.expected);
  });
});

describe("topics", () => {
  it.each(vectors.topic)("$note", (vector) => {
    expect(
      topic({ root: vector.root, contract: vector.contract, node: vector.node, path: vector.path }),
    ).toBe(vector.expected);
  });
});
