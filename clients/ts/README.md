# @alpamayo-solutions/colca-client

A Colca node's door, in TypeScript: records over HTTP — `fetch`, `ack`,
`publish`, `kv`, `self` — and live values over MQTT. No runtime dependencies
for the door, no framework, no opinion about how your service is built.

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

## Live values

`@alpamayo-solutions/colca-client/live` keeps one MQTT connection to the node's
WebSocket door for people and applications, and hands out values as they change.

```sh
npm install mqtt
```

```ts
import { Live } from "@alpamayo-solutions/colca-client/live";

const live = new Live({
  url: "wss://node:8885",
  // Asked before every connection, so hand back a token that is valid now.
  token: () => auth.freshToken(),
});

const stop = live.subscribe("steine/v1/_Metric/n-technikum/wisewoods/#", (value) => {
  show(value.topic, value.payload);
});
live.onState((state) => showOffline(state !== "online"));
```

Data and entity paths are retained at the node, so a subscription starts with
the current values and continues with the changes; nothing has to be fetched
first. Around that the client does what a page left open all day needs:

- **One connection for the whole page.** `subscribe` returns the function that
  ends that subscription and no other; the node's subscription goes when the
  last listener on a filter does.
- **A second subscriber gets the value at once.** The client keeps the last value
  of every topic, and `latest()` and `values()` read it.
- **A fresh token before the old one runs out.** The node ends a session when its
  token expires. The client reconnects shortly before, with a new token and every
  subscription sent again, and stays `online` while doing so.
- **Waits that grow after a drop**, jittered, each attempt with a fresh token.
- **A word when the subscriptions go out again.** `onResubscribe()` fires once
  they have, on every new connection — where the node's retained delivery starts
  over, and where a view reconciles a retained set from.

These are values, not a log. A change during a reconnect is superseded by the
retained value that follows it. Whatever must see every record reads a stream
through the door.

### Commands

A person's session sends commands, not values. `command()` sends one and waits
for the executor's `_Ack`:

```ts
const ack = await live.command("steine/v1/_CmdParam/n-technikum/wisewoods/line1/mas2/sta1/aggos/setGrit", {
  params: { signal: "grit", value: 120 },
});
if (ack.result_code !== 200) showRefusal(ack.message);
```

It adds the correlation id and the expiry, subscribes to the acknowledgements
before it sends, so an executor that answers at once is not missed, and matches
the answer by its id wherever in the tree it arrives. The promise settles with
the `_Ack` whatever its result code, and rejects with `CommandTimeout` when
nobody answers in time (30 s by default, which is also the command's expiry).
Offline, nothing is queued: a setpoint sent minutes late is a different
setpoint.

`publish()` sends a single record without waiting, and `newUlid()` makes ids
that sort by the time they were made, as the node's own do.

### Standing alarms

`_AlarmState` is retained, one record per alarm and an empty payload when the
alarm goes. What the node holds is therefore what stands: nothing to fetch
first, no history to fold, and no `normal` records to read past.

```ts
import { Alarms } from "@alpamayo-solutions/colca-client/live";

const alarms = new Alarms({ live, node: "n-technikum", root: "steine" });

const stop = alarms.onChange((standing) => showBanner(standing), { minSeverity: "warning" });

// Throws when the node refuses it, and when nobody answers.
await alarms.acknowledge("wisewoods/line1/mas2/gritLow", { note: "Korn getauscht" });
```

`standing()` reads the set at any time — worst first, and the oldest first
within a severity. `onChange()` is told what stands now and again on every
change, and returns the function that ends that watch and no other. An alarm's
name comes from the `_SystemElement` it hangs on, and its path is where a view
jumps to.

When the connection comes back — after a drop, and after the routine token
renewal too — the node starts its retained delivery over, and an alarm that went
while the client was away leaves nothing behind to say so. `Alarms` therefore
gives the set 750 ms to arrive again (`resyncMs`) and drops what did not come
back: a view can be that much behind the node, but it never goes on showing an
alarm that is over. It is a window, and a guess, because the node does not say
where its retained delivery ends; `resyncMs: 0` turns the reconciliation off.

`acknowledge()` sends a `_CmdAcknowledge`, and `silence(path, { minutes: 30 })`
and `unsilence()` send `_CmdOperate`, each on the alarm's own path and each
waiting for its `_Ack`. Quitting an alarm therefore needs an `acknowledge` grant,
silencing an `operate` grant (see [security](https://alpamayo-solutions.github.io/colca/security/)). The
note and the deadline ride in the payload's `command` object, where the contract
keeps a verb's arguments. The client sends no identity: who quit an alarm is the
node's word on the record, and the caller adds the note. A refusal — 300 and up — throws `AlarmRefused`
instead of resolving, because an acknowledgement that was swallowed is worse
than one that was never sent.

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
