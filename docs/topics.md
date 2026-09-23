# Topics

Every record Colca stores has a topic, and the topic says four things: which
contract the payload follows, which node owns the record, where in the plant
it belongs, and under which root the whole tree lives.

```
colca/v1/_Metric/n-edge1/line1/press3/temp
└─┬─┘ └┬┘ └──┬──┘ └──┬──┘ └───────┬───────┘
 root  │  contract  owner        path
    version
```

| Segment | Meaning |
|---|---|
| root | Chosen per deployment, `colca` unless configured otherwise. Every node of one tree uses the same root. |
| version | Always `v1` today. |
| contract | Starts with `_` and names the payload contract, for example `_Metric`, `_Signal`, `_CmdParam`, `_Ack`. |
| owner | For state and acknowledgements, the node that owns the record. It never changes on the way up. For commands, the identity the command is addressed to. |
| path | Where the record belongs. This is the only part replication rewrites. |

A topic needs at least five segments. A client publishes the absolute path
inside its own node; there is no per-client prefix to add or remove.

## Choosing the root

The root defaults to `colca`. A deployment that wants its own namespace sets
it once, and every node, service and SDK process of that tree uses the same
value:

```yaml
# node config
topic_root: acme
```

or, for any Colca process including the Python packages:

```bash
export COLCA_TOPIC_ROOT=acme
```

The environment variable wins over the config file. A root is a single topic
segment: letters, digits, `-`, `_` and `.`, starting with a letter or digit.
Nodes do not translate between roots, so a child with a different root than
its parent has its records refused.

Outside the root, a node is an ordinary MQTT broker: topics that do not start
with the root are delivered as usual and never stored.

## Ownership

A client may only publish topics whose owner segment is the node it is
connected to. A service at `n-edge1` publishes `…/_Metric/n-edge1/…`, and its
identity decides whether the path is inside the zone it may write. Ownership
survives replication: the same record at the top of the tree still says
`n-edge1`.

## Mounts

A child node speaks in its own coordinates. When its records travel up, the
parent inserts the child's mount right after the owner segment; when a command
travels down, the parent strips it again. A child only ever receives commands
for its own subtree.

| Where | Topic |
|---|---|
| a machine publishes at `n-edge1` | `colca/v1/_Metric/n-edge1/m1/temp` |
| stored at `n-edge1` | `colca/v1/_Metric/n-edge1/m1/temp` |
| stored at `n-site1` (mount `edge1`) | `colca/v1/_Metric/n-edge1/edge1/m1/temp` |
| stored at `n-global` (mount `site1`) | `colca/v1/_Metric/n-edge1/site1/edge1/m1/temp` |
| a command published at `n-global` | `colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed` |
| the command as `m1` receives it | `colca/v1/_CmdParam/m1/m1/set-speed` |
| the acknowledgement at `n-global` | `colca/v1/_Ack/n-edge1/site1/edge1/m1/set-speed` |

A mount is resolved from the system element an identity is bound to, every
time a path is built. Moving an element moves everything below it; nobody has
to be enrolled again.

## Contract classes

The contract decides which stream a record lands in and which way it flows.

| Class | Contracts | Stream | Flows |
|---|---|---|---|
| data | `_Metric` | `metrics` | up |
| entity | `_Node`, `_SystemElement`, `_Signal`, `_Constant`, `_Resource`, `_ServiceDetails`, `_EnrolledIdentity`, `_Finding`, `_AlarmState` | `entities` | up |
| definition | `_Group`, `_MetadataType`, `_AnnotationType`, `_DataModel`, `_SemanticTag` | `definitions` | down to every node |
| command | `_CmdParam`, `_CmdConfigure`, `_CmdAdmin`, `_CmdEdit`, … | `commands` | down to the target |
| acknowledgement | `_Ack` | `commands` | up |
| alarm | `_AlarmStateChange`, `_NotificationDispatched` | `alarms` | up |
| log | `_Log` | `logs` | up |
| annotation | `_Annotation` | `annotations` | up |
| audit | `_AuditEvent` | `audit` | up |

Data and entity records have a current value. They are retained on the MQTT
bus and appear in the key-value view. Commands, acknowledgements and the event
classes are history only: retaining a command would deliver it again to every
new subscriber.

A definition has no position in the plant. Its topic is
`colca/v1/_Group/{authoring-node}/{definition-id}`, nothing rewrites it on the
way down, and it looks the same on every node that holds it.

The two alarm classes are the same subject in two shapes: `_AlarmState` is the
alarm that stands right now, one retained record per alarm definition, and
`_AlarmStateChange` is the transition it went through. See [Alarms](alarms.md).

An `_Annotation` marks a span of time. Three fields place it:
`system_element_id` is the element it belongs to, `signal_ids` are the signals
it was computed from, and `related_annotation_ids` are the annotations it
belongs to, such as the panel a head pass is part of. Every listed signal lies
below the element. The element is optional, because not every producer knows
it. An annotation written through `_CmdEdit` is checked against that rule and
authorized at the element and at each signal; a producer publishing directly is
checked against the schema only. The id derives from the type, the source, the
start and the signal set, so placing an annotation or relating it does not
change its id.

## Removing a value

Publishing an empty payload to a data or entity topic retires that path. The
empty record is stored and replicated like any other, the key-value entry is
deleted, and the retained message is cleared.
