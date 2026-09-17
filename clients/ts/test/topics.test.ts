import { describe, expect, it } from "vitest";

import { parseTopic, topic, topicMatches, topicPrefix } from "../src/topics.js";

describe("matching", () => {
  it.each([
    ["a/b/c", "a/b/c", true],
    ["a/+/c", "a/b/c", true],
    ["a/+/c", "a/b/d", false],
    ["a/#", "a/b/c", true],
    ["a/#", "a", true],
    ["#", "a/b", true],
    ["a/b", "a/b/c", false],
    ["a/b/c", "a/b", false],
    ["+/b", "$SYS/b", false],
    ["#", "$SYS/b", false],
  ])("%s covers %s: %s", (filter, name, expected) => {
    expect(topicMatches(filter, name)).toBe(expected);
  });
});

describe("building", () => {
  it("takes the path as segments or pre-joined", () => {
    const parts = topic({ root: "steine", contract: "_Metric", node: "n1", path: ["mas2", "sta1", "grit"] });
    const joined = topic({ root: "steine", contract: "_Metric", node: "n1", path: "mas2/sta1/grit" });
    expect(parts).toBe(joined);
    expect(parts).toBe("steine/v1/_Metric/n1/mas2/sta1/grit");
  });

  it("puts the node under every class, definitions included", () => {
    expect(topic({ root: "steine", contract: "_AnnotationType", node: "n1", path: "01M2" })).toBe(
      "steine/v1/_AnnotationType/n1/01M2",
    );
  });

  it("allows a node-level topic", () => {
    expect(topic({ contract: "_Node", node: "n1" })).toBe("colca/v1/_Node/n1");
  });

  it("refuses a class name that is not one", () => {
    expect(() => topic({ contract: "Metric", node: "n1" })).toThrow(/_Metric/);
  });

  it("refuses wildcards and empty levels where they would change the shape", () => {
    expect(() => topic({ contract: "_Metric", node: "n1/n2" })).toThrow(/one topic level/);
    expect(() => topic({ contract: "_Metric", node: "+" })).toThrow(/one topic level/);
    expect(() => topic({ contract: "_Metric", node: "n1", path: ["a/b"] })).toThrow(/one topic level/);
  });

  it("drops empty segments instead of doubling a slash", () => {
    expect(topic({ contract: "_Metric", node: "n1", path: "a//b" })).toBe("colca/v1/_Metric/n1/a/b");
  });
});

describe("parsing", () => {
  it("takes a topic apart", () => {
    expect(parseTopic("steine/v1/_Annotation/n-technikum/panel/01M2")).toEqual({
      root: "steine",
      version: "v1",
      contract: "_Annotation",
      node: "n-technikum",
      path: "panel/01M2",
    });
  });

  it("returns nothing for what it cannot read, rather than throwing", () => {
    expect(parseTopic("steine/v1/_Metric")).toBeUndefined();
    expect(parseTopic("steine/v2/_Metric/n1/x")).toBeUndefined();
    expect(parseTopic("steine/v1/Metric/n1/x")).toBeUndefined();
  });

  it("round-trips what it built", () => {
    const built = topic({ root: "alp", contract: "_CmdConfigure", node: "n1", path: "element/upsert" });
    const parsed = parseTopic(built);
    expect(parsed?.path).toBe("element/upsert");
    expect(`${topicPrefix("alp")}${parsed?.contract ?? ""}`).toBe("alp/v1/_CmdConfigure");
  });
});
