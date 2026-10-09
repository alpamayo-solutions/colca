# Security model

Colca has no separate certificate authority and no password file. Nodes and machines
are identified by key pairs (ed25519, or ECDSA P-256 for keys held in a TPM),
people by OIDC tokens, and local services by
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

Every node and every machine owns a key pair. A machine's key is created with
`colca-keygen`; a node creates its own on first start (see
[Node keys](#node-keys)). The TLS certificate is a self-signed wrapper around
that key and carries no authority of its own, with one exception: a parent
issues each child node it approved a certificate signed with its own node key
(see [Enrollment](#enrollment)). Children already pin that key, so this adds no
trust anchor anywhere.

- A parent accepts a child when the key in the child's client certificate
  belongs to a node enrolled at the parent and the certificate is one the
  parent issued for that node and still valid. A child enrolled before issued
  certificates (`cert_state: none`) is accepted with its self-signed
  certificate until it fetches an issued one, which it does on its first start
  with this release; from then on only the issued one admits it.
- A child accepts its parent when the parent presents the key in
  `parent.pubkey`. Anything else aborts the handshake.
- A machine is accepted when its key is enrolled at the node it connects to.

TLS 1.3 is the minimum on every encrypted door. Revoking an entry closes the
live session at once: a machine's MQTT session, and every request of a child
node at the replication door, including a waiting downlink poll.

## Node keys

A node's key lives in one of two key stores, chosen by `identity.key_store`
(see [Configuration](configuration.md#identity)):

| Store | Algorithm | At `identity.key_file` | Can the private key be copied? |
|---|---|---|---|
| `file` | ed25519 for new keys; an existing ECDSA P-256 file key is kept | PKCS#8 PEM, mode 0600 | yes, by whoever can read the file |
| `tpm` | ECDSA P-256, `fixedTPM` and `fixedParent`, signing only | the key's TPM2B public and private parts; the private part is wrapped by the chip | no |

The TPM key is created under the owner hierarchy's standard ECC storage root
key, which the node re-creates as a transient object whenever it needs it; no
persistent TPM handle is used. A key blob is useless on any other TPM.

What exists at `key_file` always wins over `key_store`: a TPM blob is only
ever loaded from its TPM (the node does not start without it), a file key
stays a file key, and a file that cannot be read as a key stops the start
rather than being replaced, because a new key would be a new identity.

A public key is written as hex of its SubjectPublicKeyInfo DER, in registry
entries, `parent.pubkey` and `/healthz`. Registries written before held raw
64-hex ed25519 keys; a node rewrites its entries to SPKI on its first start
with this release, and `POST /enroll` still takes the raw form and stores it as
SPKI. `parent.pubkey` is read in either form; a child's replication cursors are
named by the raw key for an ed25519 parent whichever form the setting holds, so
changing the spelling does not restart replication.

The **fingerprint** of a key is SHA-256 over its SubjectPublicKeyInfo DER,
shown as `SHA256:` and upper-case hex pairs separated by `:`; the first four
pairs are the short fingerprint for reading out loud. In URL paths it is
written as 64 lower-case hex characters. A node shows its fingerprint and key
store in its start log, on `GET /healthz` and with `colcad identity
<config.yaml>`. `colcad tpm-identity` prints the fingerprint of the TPM's
endorsement key, which identifies the chip, without a node config.

## Identities

| Kind | How it gets in | What it may do by default |
|---|---|---|
| node | enrolled at its parent with its public key, bound to an element the parent holds | replicate its subtree |
| external | enrolled with its public key, bound to the element it belongs to | read its element's subtree; writing needs explicit grants |
| local | creates its own entry on first use of a local door, by name | read and write its element's subtree, or the whole node when unplaced |
| human | OIDC token; grants come from the groups the token names | nothing without grants; never writes data, only commands |

An identity is bound to a **system element**, not to a path. Renaming or
moving the element moves everything bound to it.

Machines and local services are enrolled through `POST /enroll` with the
admin token, or through a `_CmdAdmin` command sent down the tree to a node that
is not directly reachable. Nodes are not: they ask (below).

## Enrollment

A node with a key its parent does not know asks to join by itself: it files a
request at the parent's replication door (`POST /enroll/request`), the only
route there an unknown key may call. The parent keeps the request as pending,
outside the registry, so a pending key never authenticates, and shows it with
the key's fingerprint, its key store and the mount the node asks for. A person
decides:

| Decision | Effect |
|---|---|
| approve at an element | the node is enrolled; its next request returns its certificate |
| reject | kept as rejected for 7 days; the node stops asking until it restarts |
| block | refused until unblocked, without new records; blocking an enrolled node's key revokes it |

The person compares the fingerprint with the device: that comparison is the
security boundary for file keys and for TPM keys that were not attested. The
decision routes take a person's token with `admin:#`, never the admin token,
and every decision is an `_AuditEvent` naming the person, the fingerprint and
the key store. Requests and pre-approvals are mirrored as retained
`_EnrollmentRequest` and `_EnrollmentPreapproval` records, so the hub sees what
waits anywhere in the tree and decides through `_CmdAdmin`.

A **pre-approval** decides before the node asks: it names the node key's
fingerprint, or the fingerprint of the TPM's endorsement key, and an element.
A matching request is approved on arrival. An endorsement key matches only a
`tpm-attested` request, never a claim. Pre-approvals expire (30 days by
default) and admit a set number of requests (one by default).

The parent issues an X.509 certificate for the child's key, signed with its
own node key: subject `CN=<child ULID>`, issuer `CN=<parent ULID>`, SAN URIs
`colca:node:<ulid>`, `colca:element:<element>`, `colca:keystore:<level>` and,
when attested, `colca:ek:<manufacturer>:<serial>`. It is valid for 30 days and
renewed at two thirds of that. The registry stays the authority: a request is
admitted when its certificate is valid **and** the entry for its key is
active, so a revoke or block acts at once; expiry only makes the child
re-assert its key store monthly. An expired certificate of an active node is
renewed, not refused.

An enrolled node may move to a new key: it asks with its current key and signs
the request with the new one. Under `enrollment.key_change: auto` a move into a
TPM is accepted at once (whoever holds the current key could act as the node
already, and the new key is better bound); everything else waits for a person.
The old key is refused from that moment. A node whose configuration says
`identity.key_store: tpm` and that still holds a file key does this by itself
on start, unless children are enrolled at it: they pin its key.

## TPM attestation

Key stores are shown as `file`, `tpm` (the node says so) and `tpm-attested`
(the node proved it). To prove it, the node sends with its request the TPM's
endorsement key (EK) certificate, an attestation key and a `TPM2_Certify` of
its node key by that attestation key. The parent checks the certification,
checks the EK certificate against the TPM manufacturer certificates colcad
ships (Infineon, STMicroelectronics, Nuvoton, AMD, Intel; more with
`enrollment.tpm_roots`), and answers with a credential only that TPM can open
(`MakeCredential` to the EK, bound to the attestation key). The node opens it
with `ActivateCredential` and returns the secret. Proven with a trusted EK
certificate the key is `tpm-attested`; proven without one (a virtual TPM, for
example) it stays `tpm`. `enrollment.require` keeps requests below a level
from being approved.

## Who writes a signal's values

A write scope says where an identity may publish. It does not make the
identity a source for every signal there. A signal bound to a data tag
(`_Signal.data_tag`) takes its `_Metric` only from its **producer**: the
identity whose `_DataTags` catalogue holds that tag, at the topic that identity
publishes its catalogue on (`_DataTags/<node>/<mount>/<name>`). That covers
connectors, dataops outputs and signals created by autobind alike.

| Publisher | A `_Metric` for a bound signal |
|---|---|
| the producer | accepted |
| any other identity, local or external, whatever its grants | refused: MQTT 5 PUBACK `0x87` (not authorized), HTTP `403`, `colca_rejected_publishes_total{reason="not_producer"}`, an `_AuditEvent` denial and a log line |
| any identity, when no catalogue on the node holds the tag or two claim it | refused until exactly one catalogue holds it |
| the admin token (`/publish` with `X-Colca-Token`) | accepted, stored with `written_by: admin`, and logged |
| replication from a child | accepted: the child admitted it |

A signal bound to nothing, or a `_Metric` on a path with no signal, keeps the
write-scope rule alone.

When a configure command moves a signal (a rename, a reparent, or a
`_CmdEdit` placement), the node moves the signal's current `_Metric` with it:
the same payload and timestamp at the new path, a tombstone at the old one.
When it moves an element, the catalogue of a service placed there moves to the
element's new path, so the service stays its signals' producer. Nothing new is
written for the producer; a value already published at the new path is kept.

The local door authenticates by reaching it, not by a secret, so this rule
stops a misconfigured or misbehaving service. It does not stop a process that
connects under the producer's name.

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
and metadata of an existing constant, through an `update` or a single-key
`metadata` edit (see [concepts.md](concepts.md#writing-one-metadata-key)) — an
operator input such as a station's sandoff or grit — never creating or deleting
a constant, never its other attributes, and never a signal's binding. Both are
scoped by the grant's element exactly as `configure` is:
`cmd:<element>/#:param` reaches only the constants under that element. The write itself is still made by the node —
`_CmdEdit` never lets a person publish state directly — but it carries the
operator's own verified identity as `actor_id`/`actor_label`/`actor_kind`, so
`/kv` can show who set it (see [http-api.md](http-api.md)).

An external reference is stored at the reserved path
`_colca/external-references/<id>`, outside every element, so `_CmdEdit`
authorizes it at the entity it belongs to: `cmd:<element>/#:configure` covers
adding, changing and removing the references of the entities under that
element, and deleting such an entity together with its references. A changed
or removed reference also counts at the entity it belongs to now, so it cannot
be moved away from an entity outside the grant.

`#` in place of an element means the whole node. A grant on an element the node
has never heard of covers nothing, and a node that has never reached its parent
cannot resolve elements above itself, so scoped grants fail closed there.

### Grants relative to the node a person signs in at

A grant names an element by id, so a group that should give every edge's
operators their own machine would need one element id per machine. A person's
grant can instead name `$node`, the node they signed in at:

| Zone | Covers, at the node the person signed in at |
|---|---|
| `$node/#` | everything that node holds, as `#` does there |
| `$node/<path>/#` | the subtree at that local path, for example `$node/Line1/Press/#` |

One definition, written once at the root, then works for the whole fleet:
`read:$node/#` lets an operator signed in at edge 1 read edge 1 and one signed
in at edge 2 read edge 2. `$node` resolves when the grant is used, at the node
evaluating it; the `_Group` definition descends unchanged.

- `$node` is the node whose door verified the person's token, or whose own
  local service attested their groups. A command that came down from the
  parent was admitted at another node, so its sender's `$node` grants cover
  nothing at the node executing it. Use element grants for people who command
  edges from the hub.
- At the root, `$node/#` is the whole tree. Give a group with `$node` grants
  only to people who sign in at the edges; when edges share one identity
  provider with each other, a member who can sign in at an edge holds that
  edge.
- `<path>` is a path of names in the node's own frame. Unlike an element id it
  follows a rename: renaming `Line1` moves what `$node/Line1/#` covers. A path
  no element sits at covers nothing.
- Only people hold `$node` grants. Enrollment refuses them for machines,
  services and nodes, whose placement is their zone. `admin:$node` is
  reserved like every zone-scoped `admin`.
- Nodes released before node-relative grants refuse a definition that holds a `$node` grant.
  Upgrade the edges before defining such a group.

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
admin token (`X-Colca-Token`) or a token with `admin:#`. The enrollment
decisions (`/enroll/requests`, `/enroll/preapprovals`) take only a person's
token with `admin:#`. An empty `api.token`
authenticates nobody; it does not switch the check off.

## Secrets

With `secrets_dir` set, a local service can store sealed envelopes on its node
(`/secrets`). The node only ever holds ciphertext; the service owns the private
key and decrypts in its own process. The secret store is a separate database,
never replicated and not part of `data_dir`. See the `secrets` Go package.

## Reporting a vulnerability

See [SECURITY.md](https://github.com/alpamayo-solutions/colca/blob/main/SECURITY.md).
