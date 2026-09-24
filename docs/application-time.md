# Application time

Colca's operational clock remains real UTC. Authentication, command expiry,
retention, transport timeouts and health checks do not run in simulation time.
NTP is optional and belongs to the host. The existing live `_TimeSync/<node>`
beacon exposes the node's authoritative real time; HTTP stream responses expose
the same clock in `now_ms`. A consumer selects one real-time source and does not
add an MQTT correction to an NTP correction.

An optional `_ClockDefinition/<authority>/<id>` is a normal definition, written
through `_CmdConfigure/<authority>/definition/upsert` and replicated to children
like other definitions. Applications opt into one exact topic. The broker never
applies this definition to its own operational clock.

```json
{
  "id": "factory",
  "run_id": "experiment-1",
  "revision": 1,
  "real_anchor": 1790208000,
  "factory_anchor": 1758672000,
  "start_at": 1758672000,
  "rate": 100,
  "stop_at": 1790208000,
  "catch_up": false
}
```

Timestamps are UTC Unix seconds. Application time is `factory_anchor +
max(0, real_now - real_anchor) * rate`, capped at `stop_at` when present.
`rate: 0` pauses the timeline. Catch-up also caps application time at real UTC,
so it continues at 1× when the history reaches the present.

Changes increment `revision` by one and preserve time at the new real anchor.
Exact retries are idempotent; stale revisions, rewinds and run identity changes
are rejected. `start_at` is the immutable original run start so a restarted
worker can distinguish it from a later speed-change anchor. Use a new clock ID
and a deliberately separate data namespace for a new run.

A future-effective change can carry `previous` with the old segment's
`real_anchor`, `factory_anchor`, `rate`, `stop_at` and `catch_up`. Consumers use
that segment until the new real anchor. This also works for a consumer joining
between publication and activation. Controllers should wait for an already
scheduled transition before scheduling another.

The Chaski SDK evaluates definitions and the existing `_TimeSync` beacon.
Its MQTT authority must be fresh; losing authority pauses application work
instead of silently switching timelines. A requested rate is a time mapping,
not a throughput guarantee: applications must measure execution progress and
handle backpressure before claiming complete accelerated history.

See [contracts](contracts.md), [HTTP API](http-api.md) and
[operations](operations.md) for the underlying publication and transport paths.
