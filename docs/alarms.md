# Alarms

An alarm shows up in two shapes. `_AlarmState` is the alarm that stands right
now; `_AlarmStateChange` is a transition it went through. They answer different
questions and are kept apart on purpose.

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
| `silenced_by`, `silenced_until` | optional | |

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

1. The person sends a `_CmdOperate` for the alarm.
2. The door authenticates them and stores the command with its authorship
   envelope. The node witnesses the actor; `GET /fetch` hands `actor_id`,
   `actor_label` and `actor_kind` to the consumer.
3. The evaluator consumes the command and writes the next `_AlarmState`,
   putting the witnessed `actor_id` into `acknowledged_by`, with
   `acknowledged_at` and, if there was one, the `note`.

So `acknowledged_by` is always a subject the node vouched for, never one the
writer claimed for itself. A silence works the same way.
