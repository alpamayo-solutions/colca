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

Command classes are `acknowledge`, `param`, `operate`, `maintain`, `configure`
and `admin`. `configure` is separate on purpose: someone who may rename a signal
must not thereby be able to send maintenance commands to a PLC. `acknowledge`
is separate for the opposite reason: quitting an alarm moves nothing, so
everyone who watches a line may hold it, and holding it must not let them start
or stop the line. It covers `_CmdAcknowledge` and nothing else; silencing an
alarm keeps notifications from other people and stays `operate`. No class
implies another: someone who may both operate and acknowledge holds both.

`_CmdEdit` is a person's tool for the node's data model, and the door admits
anyone holding `configure` on it. Two narrower classes are also admitted, each
covering only the part of `_CmdEdit` that matches the hazard: `operate` covers
creating an annotation, or editing one's own; `param` covers setting the value
and metadata of an existing constant — an operator input such as a station's
sandoff or grit — never creating or deleting a constant, never its other
attributes, and never a signal's binding. Both are scoped by the grant's
element exactly as `configure` is: `cmd:<element>/#:param` reaches only the
constants under that element. The write itself is still made by the node —
`_CmdEdit` never lets a person publish state directly — but it carries the
operator's own verified identity as `actor_id`/`actor_label`/`actor_kind`, so
`/kv` can show who set it (see [http-api.md](http-api.md)).

`#` in place of an element means the whole node. A grant on an element the node
has never heard of covers nothing, and a node that has never reached its parent
cannot resolve elements above itself, so scoped grants fail closed there.

## People

People authenticate with tokens from OIDC issuers the node lists. The node
validates them offline:

```yaml
auth:
  issuers:
    - url: https://login.example.com/realms/plant
  audience: colca
  jwks_url: https://login.example.com/realms/plant/protocol/openid-connect/certs
mqtt_human: { tcp_addr: ":8884", ws_addr: ":8885" }
```

A token's `iss` must be one of `issuers`, and its `aud` must be `audience`.
The issuer also decides which keys the signature is checked against: its own
`jwks_url`, or the shared `auth.jwks_url` when it has none. A token that names
an issuer but was signed with another issuer's keys is rejected.

Several issuers are normal when one identity provider is reached under more
than one host name. Keycloak, for example, writes the host the browser used
into `iss`, so a site with two networks gets two issuers with the same keys:

```yaml
auth:
  issuers:
    - url: https://red.plant.example/realms/plant
    - url: https://green.plant.example/realms/plant
  audience: colca
  jwks_url: http://keycloak:8080/realms/plant/protocol/openid-connect/certs
```

Issuers from different identity providers each name their own keys:

```yaml
auth:
  issuers:
    - url: https://login.example.com/realms/plant
      jwks_url: https://login.example.com/realms/plant/protocol/openid-connect/certs
    - url: https://idp.partner.example
      jwks_url: https://idp.partner.example/.well-known/jwks.json
  audience: colca
```

Signing keys are fetched in the background from each distinct JWKS URL,
stored, and refreshed when a token names an unknown key. The issuer is never called while a request waits, and a
node that restarts without network keeps validating until the tokens expire. A
session ends when its token expires, unless the client renews it first.

An MQTT 5 client renews on the open connection: it sends the authentication
method `colca-token` in its CONNECT (the token stays the password), and the node
names the method in the CONNACK. Before the token runs out, the client sends an
AUTH packet with reason `0x19`, the same method, and the new token as
authentication data. The node checks it as at CONNECT, requires the same `sub`,
and moves the session's grants and expiry to it; the subscriptions stay. A
refused token ends the connection with `0x87` (not authorized), a different
method with `0x8C`. A client without the method reconnects with a new token
instead.

A logout at the identity provider reaches the node by OIDC back-channel logout.
The node serves `POST /auth/backchannel-logout` on its API door and its local
door; the identity provider posts a signed `logout_token` there. The node checks
the signature against the issuer's keys, `iss`, `aud` (the `audience` above),
`iat`, `exp` if present, the back-channel logout event in `events`, that there
is no `nonce`, and that the `jti` was not used before, and answers 400 to a token
that fails. A valid one is answered 200: every connection of that session (the
token's `sid`) is closed with `0x98` (administrative action), and tokens of the
session are refused from then on, on every door. A logout token with a `sub` and
no `sid` ends everything issued to that person before it. In Keycloak, set the
client's back-channel logout URL to the node's local door, for example
`http://colca/auth/backchannel-logout`. Without back-channel logout, the token
lifetime is the revocation delay.

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
