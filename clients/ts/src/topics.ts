/**
 * Colca topics: `{root}/v1/_{Contract}/{node}/{path…}`.
 *
 * Level 4 is the publishing node for every class, definitions included — the
 * broker reads the node off that level whatever the contract is, and a topic
 * without it is refused. A client that special-cases definitions builds topics
 * the node will not take.
 */

/** The topic version this client speaks. */
export const TOPIC_VERSION = "v1";

/** The root used when none is given. Deployments override it (`steine`, `alp`). */
export const DEFAULT_ROOT = "colca";

export interface TopicParts {
  root: string;
  version: string;
  /** With the leading underscore, as it appears on the wire: `_Metric`. */
  contract: string;
  node: string;
  /** Everything below the node, joined with `/`. Empty for a node-level topic. */
  path: string;
}

export interface TopicOptions {
  contract: string;
  node: string;
  /** One pre-joined path, or the segments; both render the same. */
  path?: string | readonly (string | number)[];
  root?: string;
}

const SEGMENT = /^[^/+#]+$/;

/** Build a topic. Throws on a segment that would change the topic's shape. */
export function topic({ contract, node, path = [], root = DEFAULT_ROOT }: TopicOptions): string {
  if (!contract.startsWith("_")) {
    throw new Error(`contract must be a class name such as _Metric, got ${JSON.stringify(contract)}`);
  }
  check("root", root);
  check("node", node);
  const segments = (typeof path === "string" ? path.split("/") : path.map(String)).filter((s) => s !== "");
  for (const segment of segments) check("path segment", segment);
  return [root, TOPIC_VERSION, contract, node, ...segments].join("/");
}

/** The prefix every topic under `root` starts with, wildcards included. */
export function topicPrefix(root: string = DEFAULT_ROOT): string {
  check("root", root);
  return `${root}/${TOPIC_VERSION}/`;
}

/**
 * Take a topic apart. Returns undefined rather than throwing: a subscriber sees
 * whatever the broker delivers, and an unparseable topic is a routing decision,
 * not an error.
 */
export function parseTopic(value: string): TopicParts | undefined {
  const parts = value.split("/");
  if (parts.length < 4) return undefined;
  const [root, version, contract, node, ...rest] = parts;
  if (version !== TOPIC_VERSION || !contract.startsWith("_")) return undefined;
  return { root, version, contract, node, path: rest.join("/") };
}

function check(what: string, value: string): void {
  if (!SEGMENT.test(value)) {
    throw new Error(`${what} must be one topic level without + or #, got ${JSON.stringify(value)}`);
  }
}
