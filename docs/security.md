# Security model

Colca has no certificate authority and no password file. Nodes and machines
are identified by ed25519 keys, people by OIDC tokens, and local services by
the fact that they can reach a door that is never published.

## Doors

A node opens only the listeners its configuration names. Each one has a
single way of deciding who is talking.

| Door | Default port | Who uses it | Credential |
|---|---|---|---|
| local HTTP | 80 | services inside the deployment network | none; the door is not published |
| local MQTT | 1883 | the same | none |
| API | 443 | machines, external services, administrators, people | client certificate, `Authorization: Bearer`, or the admin token |
| machine MQTT | 8883 | machines and external services | client certificate carrying an enrolled key |
| human MQTT | 8884 | people and applications | OIDC token as the MQTT password |
| human WebSocket | 8885 | browsers | OIDC token |
| replication | 9443 | child nodes | client certificate carrying an enrolled node key |

A presented credential that fails is refused. There is no fallback to a
weaker door.

The local doors are meant for services that run next to the node, for example
in the same Docker network. Publish them, and anyone who reaches them can
write. Keep them unpublished.

## Keys instead of a CA

Every node and every machine owns an ed25519 key pair, created with
`colca-keygen`. The TLS certificate is a self-signed wrapper around that key
and carries no authority of its own.

- A parent accepts a child when the key in the child's client certificate
  belongs to a node enrolled at the parent.
- A child accepts its parent when the parent presents the key in
  `parent.pubkey`. Anything else aborts the handshake.
- A machine is accepted when its key is enrolled at the node it connects to.

TLS 1.3 is the minimum on every encrypted door. Revoking an entry closes the
live session at once.

## Identities

| Kind | How it gets in | What it may do by default |
|---|---|---|
| node | enrolled at its parent with its public key, bound to an element the parent holds | replicate its subtree |
| external | enrolled with its public key, bound to the element it belongs to | read its element's subtree; writing needs explicit grants |
| local | creates its own entry on first use of a local door, by name | read and write its element's subtree, or the whole node when unplaced |
| human | OIDC token; grants come from the groups the token names | nothing without grants; never writes data, only commands |

An identity is bound to a **system element**, not to a path. Renaming or
moving the element moves everything bound to it.

Enrollment goes through `POST /enroll` with the admin token, or through a
`_CmdAdmin` command sent down the tree to a node that is not directly
reachable.

## Grants

Grants use one grammar for machines and people:

| Grant | Meaning |
|---|---|
| `read:<element>/#` | live bus, current state and history below the element |
| `write:<element>/#` | publish data and entities below the element |
| `cmd:<element>/#:<classes>` | send commands of the listed classes below the element |
| `admin:#` | use the administrative routes; does not widen reads or commands |

Command classes are `param`, `operate`, `maintain`, `configure` and `admin`.
`configure` is separate on purpose: someone who may rename a signal must not
thereby be able to send maintenance commands to a PLC.

`#` in place of an element means the whole node. A grant on an element the node
has never heard of covers nothing, and a node that has never reached its parent
cannot resolve elements above itself, so scoped grants fail closed there.

## People

People authenticate with tokens from any OIDC issuer. The node validates them
offline:

```yaml
auth:
  issuer: https://login.example.com/realms/plant
  audience: colca
  jwks_url: https://login.example.com/realms/plant/protocol/openid-connect/certs
mqtt_human: { tcp_addr: ":8884", ws_addr: ":8885" }
```

Signing keys are fetched in the background, stored, and refreshed when a token
names an unknown key. The issuer is never called while a request waits, and a
node that restarts without network keeps validating until the tokens expire. A
session ends when its token expires; the token lifetime is therefore the
revocation delay.

A token names groups, not grants. The node resolves the groups against
`_Group` definitions that its ancestors pushed down. Membership lives in the
identity provider, grants live in the tree, and an edge cut off from its parent
still knows what its people may do. `colca-grantsync` keeps a Keycloak realm and
the tree's groups in step, if you use Keycloak.

## Administration

The administrative routes (`/enroll`, `/debug/state`) require either the node's
admin token (`X-Colca-Token`) or a token with `admin:#`. An empty `api.token`
authenticates nobody; it does not switch the check off.

## Secrets

With `secrets_dir` set, a local service can store sealed envelopes on its node
(`/secrets`). The node only ever holds ciphertext; the service owns the private
key and decrypts in its own process. The secret store is a separate database,
never replicated and not part of `data_dir`. See the `secrets` Go package.

## Reporting a vulnerability

See [SECURITY.md](https://github.com/alpamayo-solutions/colca/blob/main/SECURITY.md).
