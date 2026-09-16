# @alpamayo-solutions/colca-client

A Colca node's door, in TypeScript. Reads and writes records over HTTP —
`fetch`, `ack`, `publish`, `kv`, `self` — and nothing else: no runtime
dependencies, no framework, no opinion about how your service is built.

```sh
npm install @alpamayo-solutions/colca-client
```

## A consumer

```ts
import { Door, Stream } from "@alpamayo-solutions/colca-client";

const door = new Door({ baseUrl: "http://colca", service: "my-app" });
const panels = new Stream(door, "annotations", door.cursorName("panels"), {
  prefix: "wisewoods/line1",
});

for await (const record of panels.follow()) {
  // Acked page by page: a handler that throws sees its page again.
  console.log(record.topic, record.payload);
}
```

## A producer

```ts
await door.publishTo(
  { root: "steine", contract: "_Metric", node: "n-technikum", path: "wisewoods/line1/mas2/grit" },
  { signal_id: "01M2AB…", timestamp: Date.now() / 1000, value: 60 },
);
```

## Which door, which credential

| Door      | URL                                                     | Credential                                                        |
| --------- | ------------------------------------------------------- | ----------------------------------------------------------------- |
| local     | `http://colca` (port 80, inside the deployment network) | none — reachability is the credential; `service` names the caller |
| published | `https://node:443`                                      | a person's bearer token, or a machine's pinned client certificate |

A client certificate is a TLS matter, so it belongs to the runtime rather than
to this package: hand in a `fetch` that carries it (`options.fetch`).

## Things the node insists on

- **Cursors belong to their caller.** They are named `c/{service}/…`, which is
  what `door.cursorName()` builds; the node refuses any other name.
- **Fetching never moves a cursor.** Only `ack` does, and only forward. `tail`
  reads the end of a stream without touching it, which is what a view wants.
- **A gap is not an error.** When records were pruned below the cursor, the page
  says so. `Stream` passes it to `onGap` and acks past it, so it is reported
  once rather than forever.
- **The node decides what you see.** `/fetch` and `/kv` filter by the caller's
  read grants; a publish is judged against its zone and grants. A 403 carries
  the node's reason.

## Ids

`deriveAnnotationId()` computes an annotation's id the way the node's own
contracts package does. It is derived rather than drawn so that create, update
and delete of one annotation are appends under the same id — a producer that
runs twice overwrites its own record instead of doubling it.

The agreement is checked against `clients/spec/vectors.json`, generated from
`colca-data-contracts`. If a rule changes there, the tests here fail.

## Development

```sh
npm install
npm run lint
npm run typecheck
npm test
npm run build
```

The wire format itself is documented in
[HTTP API](https://alpamayo-solutions.github.io/colca/http-api/).
