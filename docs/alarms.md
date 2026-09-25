# Alarms

An alarm shows up in three shapes. `_Finding` is what a service found and stands
behind; `_AlarmState` is the alarm that stands right now; `_AlarmStateChange` is
a transition it went through. They answer different questions, have different
writers, and are kept apart on purpose.

## What a service found

`_Finding` is entity-class state written by the service that ran the check. It
republishes the record for as long as the finding holds and retires the path
when it no longer does. It has no memory of what it said and no way to learn
whether anybody acknowledged anything — "still broken" is the whole of its job.

```
colca/v1/_Finding/{node}/{element-path}/{finding-name}
```

The unusual part is that the HANDLING travels with the observation:
`silenceable`, `dwell_on_s`/`dwell_off_s`, `min_repeat_s`, `remedy`. The service
that invented the check is the only one that knows whether its condition flaps,
how long it must hold to mean anything, or whether an operator may reasonably
silence it. A rule kept somewhere else has to be matched back to the finding,
and the matching is what drifts.

`suggested_severity` is named as a proposal because that is what it is. The
manager decides, and an operator may decide differently.

### Why it is not the alarm

`_AlarmState` carries the lifecycle — `acknowledged_by`, `silenced_until` — and
a record replaces the one before it. A service republishing its observation into
that record would wipe the operator's acknowledgement on every cycle. One writer
per record: the service writes findings, the manager reads them and owns the
alarm.

`finding_seen_at` on the alarm is the other half of that split. A service that
stops writing without retiring its path — it crashed, the network went — leaves
a timestamp that stops advancing, and the manager can move the alarm to
`unknown` rather than showing `firing` forever.

## The standing alarm

`_AlarmState` is entity-class state: one record per alarm definition, at the
alarm's own place in the tree.

```
colca/v1/_AlarmState/{node}/{element-path}/{alarm-name}
```

The alarm is an element like any other, not a reserved `_colca/…` path, so read
grants and zones reach it the same way they reach a signal below the same
machine.

Because there is one record per alarm, each write replaces the one before it.
A client that has just connected subscribes to `_AlarmState/#`, or reads `GET
/kv`, and has the complete picture at once — no database beside the bus.

| Field | | |
|---|---|---|
| `alarm_id` | ULID, required | the alarm definition |
| `status` | required | `pending`, `firing` or `unknown` |
| `severity` | required | `info`, `warning` or `critical` |
| `since` | required | unix seconds this status has held |
| `signal_id` | ULID, required | the signal it is about |
| `reason` | required | `threshold`, `no_data`, `stream_gap`, … |
| `value`, `op`, `threshold` | optional | the measurement and the rule, so a reader can render "82.4 > 80" |
| `event_id` | optional | the `_AlarmStateChange` this state came out of |
| `acknowledged_by`, `acknowledged_at`, `note` | optional | the receipt |
| `silenced_by`, `silenced_until` | optional | copied from the `_AlarmSilence` that covers it |
| `keep_after_read_s`, `keep_listed_after_read_s`, `keep_after_clear_s` | optional | how long a notification list keeps it, as the manager resolved it from the finding |
| `acknowledged_by_name`, `silenced_by_name` | optional | readable names beside the subjects, for display |

`status` and `severity` are closed vocabularies in the schema bundle. `reason`
is a free string on purpose: `threshold`, `no_data` and `stream_gap` are what
the evaluator writes today, and a derived diagnosis can name its own.

## Gone is an empty payload

There is no `normal` status and no `recovered` status. An alarm that no longer
stands is retired with a tombstone, the empty payload that retires any entity
path: the key-value entry is deleted and the retained message is cleared. What
is not in the key-value view is not standing.

Over MQTT that is a zero-length message. Over `POST /publish` it is a body that
leaves `payload` out altogether — a JSON `null` is a value, and the schema
refuses it.

That is also why the standing alarm has to be one record per definition. A
transition cannot be state — as a state class, every transition would leave a
key-value entry at a path nothing ever writes again, on every ancestor node
too. The history of transitions stays where it belongs, appended to the
`alarms` stream, and is read with a cursor over `GET /fetch`.

## Silences

A silence is its own record, not a field of one alarm:

```
colca/v1/_AlarmSilence/{node}/{element-path}/{reason}
```

One per element and reason, with `until`, `silenced_by`, `silenced_at` and
optionally `silenced_by_name` and `note`. The manager writes it on a person's
silence command, copies `until` onto every alarm at that element with that
reason as `silenced_until`, and retires it with a tombstone when it runs out
or is ended. It outlives the alarm: one that clears and fires again while the
silence runs is silenced from its first record. Alarms still fire and are
recorded while silenced.

## What is not in the payload

**No recipients.** Who is notified is written by someone else, changes for
reasons that have nothing to do with the plant, and is personal data that does
not belong in a retained record that never expires on its own. A policy edit
must not rewrite an alarm's state. Who was actually reached, over which
channel and when, is in `_NotificationDispatched` on the `alarms` stream.

**No `title`, `message` or `deepLink`.** The alarm's name comes from its
`_SystemElement`, and the topic path already is the link.

## How an acknowledgement gets in

A person cannot write the state record: acknowledging is an act, and the
payload must not assert who performed it.

1. The person sends a `_CmdAcknowledge` for the alarm, on the alarm's own
   path with the verb `ackAlarm` last. It needs an `acknowledge` grant that
   covers the alarm's element, not `operate` (see [security.md](security.md)).
2. The door authenticates them and stores the command with its authorship
   envelope. The node witnesses the actor; `GET /fetch` hands `actor_id`,
   `actor_label` and `actor_kind` to the consumer.
3. The evaluator consumes the command and writes the next `_AlarmState`,
   putting the witnessed `actor_id` into `acknowledged_by` and its
   `actor_label` into `acknowledged_by_name`, with `acknowledged_at` and, if
   there was one, the `note`.

So `acknowledged_by` is always a subject the node vouched for, never one the
writer claimed for itself. For a person, `actor_label` is the token's
`preferred_username`; the name is for display, the subject is the identity. A silence works the same way, but as a `_CmdOperate`
(`silenceAlarm`, `unsilenceAlarm`): keeping an alarm from notifying anyone is
more than confirming one has seen it, so it needs `operate`.
