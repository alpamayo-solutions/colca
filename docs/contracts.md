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

## Catalog records

A catalog is master data kept in `_Constant` records of data type `json`: one
product per `catalog/products/{sku}`, one recipe per `catalog/recipes/{id}`. The
node checks only that such a value is JSON, and no bundle applies to it; the
record's shape is its own contract, `colca_data_contracts.catalog` (dataclasses
plus `PRODUCT_SCHEMA` and `RECIPE_SCHEMA`), and `CatalogProduct` and
`CatalogRecipe` in the TypeScript client.

```json
{
  "sku": "AMALQ301H6", "name": "MDF 18 mm", "substrate": "MDF",
  "thickness_mm": 18.7, "length_mm": 4100, "width_mm": 2070, "sandoff_mm": 0.7,
  "density_kg_m3": 745, "final_grit": 150, "recipe_id": "recipe-60-80-100-120-150",
  "is_default": true,
  "attributes": {"sap_plant": "BSK1"},
  "provenance": {"source": "erp", "set_by": "sap-bridge", "set_at": "2026-09-25T08:00:00Z"},
  "field_provenance": {"density_kg_m3": {"source": "manual", "set_by": "Admin A", "set_at": "2026-09-25T09:12:00Z"}}
}
```

- A product names its recipe by `recipe_id`: recipes are shared, and one edit
  changes one record.
- Anything beyond the standard fields goes in `attributes`; an unknown top-level
  field is refused.
- `provenance` is the record's writer. `field_provenance` names the fields
  someone else set on top of it, such as a density entered by hand. A writer
  that replaces the whole record keeps those fields.

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
| `catalog_records.json` | product and recipe records, Python and TypeScript |

Change a rule by changing its vector first, then make both sides pass.
