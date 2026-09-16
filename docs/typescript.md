# TypeScript SDK

`@alpamayo-solutions/colca-client` talks to a node's door from TypeScript: read
records, ack them, publish, scan the retained values, ask who you are. It has no
runtime dependencies — the platform's `fetch` does the work — and ships as ESM
for Node 22.12 and newer, and for browsers.

```sh
npm install @alpamayo-solutions/colca-client
```

## Reading

```ts
import { Door, Stream } from "@alpamayo-solutions/colca-client";

const door = new Door({ baseUrl: "http://colca", service: "my-app" });
const panels = new Stream(door, "annotations", door.cursorName("panels"), {
  prefix: "wisewoods/line1",
});

for await (const record of panels.follow()) {
  console.log(record.topic, record.payload);
}
```

`follow()` drains the stream and acks each page once its records have been
consumed, then waits and drains again. A handler that throws sees its page
again, so handlers must survive running twice on the same record.

For a view that wants the newest records rather than the next ones, `tail()`
reads the end of the stream without moving the cursor — a view and a consumer
can therefore share a cursor name.

## Writing

```ts
await door.publishTo(
  { root: "steine", contract: "_Metric", node: "n-technikum", path: "wisewoods/line1/mas2/grit" },
  { signal_id: "01M2AB…", timestamp: Date.now() / 1000, value: 60 },
);
```

The node judges a publish by the caller's zone, identity and grants, exactly as
it judges an MQTT publish. A refusal is a `DoorError` carrying the node's own
reason.

## Which door, which credential

| Door | URL | Credential |
| --- | --- | --- |
| local | `http://colca` (port 80, inside the deployment network) | none — reachability is the credential; `service` names the caller |
| published | `https://node:443` | a person's bearer token, or a machine's pinned client certificate |

A client certificate is a TLS matter and belongs to the runtime: hand in a
`fetch` that carries it (`options.fetch`).

## Contracts and ids

The package carries the contracts as types, generated from the same bundle the
node validates against, so a field the types do not know is a field the node
would refuse:

```ts
import type { PayloadOf } from "@alpamayo-solutions/colca-client";

const metric: PayloadOf<"_Metric"> = { signal_id: "01M2AB…", timestamp: 0, value: 1 };
```

`deriveAnnotationId()` computes an annotation's id the way the Python contracts
package does — derived rather than drawn, so that create, update and delete of
one annotation are appends under the same id. Both implementations are held to
the vectors in `clients/spec/vectors.json`; if a rule changes, the clients fail
rather than drift.

The wire format itself is described under [HTTP API](http-api.md).
