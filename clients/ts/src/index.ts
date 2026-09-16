/**
 * colca-client — the door, in TypeScript.
 *
 * ```ts
 * const door = new Door({ baseUrl: "http://colca", service: "my-app" });
 * const stream = new Stream(door, "annotations", door.cursorName("panels"));
 * for await (const record of stream.follow()) {
 *   console.log(record.topic, record.payload);
 * }
 * ```
 */

export { Door, DoorError } from "./door.js";
export type {
  Attribution,
  DoorOptions,
  DoorRecord,
  FetchOptions,
  Gap,
  KvEntry,
  Page,
  PublishResult,
  SelfInfo,
} from "./door.js";

export { Stream } from "./stream.js";
export type { FollowOptions, StreamOptions } from "./stream.js";

export { DEFAULT_ROOT, TOPIC_VERSION, parseTopic, topic, topicPrefix } from "./topics.js";
export type { TopicOptions, TopicParts } from "./topics.js";

export { annotationIdMaterial, deriveAnnotationId, ulidFromBytes } from "./ids.js";
export type { AnnotationIdInput } from "./ids.js";

// The 32 contracts, straight from the bundle the node validates against.
export * from "./generated/contracts.js";
