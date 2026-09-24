# Contracts

A contract is the shape of a payload together with the rules that come with its
name: which stream a record lands in, which way it flows through the tree, and
whether it has a current value. The shapes live in one place, the
`colca-data-contracts` package in `contracts/`. Nodes do not contain them; they
enforce a schema bundle generated from them.

## The schema bundle

```bash
make bundle        # writes build/bundle/contracts-bundle.json and prints its sha256
```

The bundle is canonical JSON: a manifest and a restricted JSON Schema for every
contract. Generating it again from the same source gives the same bytes, so its
digest identifies it.

A node loads the bundle when it starts, from `contracts.bundle`
(`/etc/colca/contracts-bundle.json` by default; the image contains the bundle of
its own commit). `contracts.sha256` pins the expected digest, and a node with a
different bundle does not start.

- **A bundle replaces the built-in checks.** It never merges with them. A contract
  that is not in the bundle is refused. A node without a bundle applies small
  built-in checks for its core contracts only.
- **The bundle carries the routing class** of each contract, so a new contract
  can be introduced by shipping a bundle, without a new broker. A new command
  contract counts as an `admin` command until the domain package names its class.
- **Some contracts belong to the node.** `_StreamGap`, `_EnrolledIdentity` and
  `_TimeSync` are produced and checked by the node itself; a bundle that
  declares one does not load.
- **Validation happens once**, at the door where a record enters. Replication
  does not validate again, so nodes on different bundle versions keep
  replicating while a rollout moves through the tree.

`colca_contracts_bundle_info{version,digest,source}` shows which rules a node
enforces.

## Changing a contract

1. Change the payload class in `contracts/src/colca_data_contracts/payload.py`.
   A new contract also needs its class in `routing.py`.
2. `make contracts-test`, then `make bundle`.
3. `make test`. The Go tests that judge the doors read the generated bundle.

## Golden vectors

For the optional virtual clock definition and its continuity rules, see
[application time](application-time.md).

Where Go and Python both need a rule natively, a vector file in
`contracts/src/colca_data_contracts/vectors/` holds the cases and the expected
answers, and the tests on both sides read the same file:

| File | Pins |
|---|---|
| `topic_transformations.json` | the topic grammar and mount rewriting |
| `grant_evaluation.json` | how a person's grants are evaluated |
| `authz_objects.json` | the Keycloak objects a grant is made of |
| `annotation_id.json` | how an annotation's id is derived |
| `data_model_vocabulary.json`, `signal_data_types.json` | the data type vocabularies |
| `metric_rows.json` | which historian column a metric value lands in |
| `application_time.json` | real-to-application clock projection in Go and Chaski |
| `sanitize.json` | how names become path segments |
| `service_context.json`, `log_payload.json`, `manifest_streams.json` | service identity, log records, stream manifest |

Change a rule by changing its vector first, then make both sides pass.
