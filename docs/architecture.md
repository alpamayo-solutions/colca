# Architecture

## Repository layout

```
cmd/            the binaries: colcad and the tools that run beside it
internal/       the core: store, ingest engine, doors, replication, retention
plugins/uns/    the domain: topic grammar, contract classes, grants, identities
door/           Go client for services that talk to a node's local door
secrets/        sealed-envelope helpers for services that store secrets on a node
contracts/      the Python data contracts, the bundle generator, golden vectors
bench/          benchmark scenarios and their thresholds
deploy/         image, demo topology and its configuration templates
demo/           the smoke test and the narrated demo
tests/          multi-node tests that run in one process
pywheel/        packaging of colcad for chaski[node]
scripts/        build and repository checks
```

`internal/` knows how to store, move and guard records. `plugins/uns/` knows
what the records mean. The core imports the domain package; the domain package
imports nothing but the Go standard library.

## Design principles

Comments in the code refer to these by number.

### 1. One way to do one thing

If there are two ways to do something, one of them is a bug. A change replaces
the old path instead of adding a parallel one, and is not finished until the
old path, its helpers and its tests are gone.

Things that differ in exactly one dimension share one representation and one
code path, and differ only at that seam: human and machine grants are parsed,
stored and enforced by the same code, and only authentication differs. The
counter-rule is just as important: things that merely look alike stay apart.
If a change to one would not always require the same change to the other, they
are two things.

### 2. One owner per fact

Every fact has exactly one definition. Where the same knowledge must exist in
two places, it is generated or pinned from that definition, never written
twice by hand.

| Tier | Example | Rule |
|---|---|---|
| Definition | contract shapes in `contracts/`, the grant syntax | One source; everything downstream is generated from it, like the schema bundle. |
| Necessary duplication | the topic grammar in franzmq (Python) and `plugins/uns` (Go) | Only where both sides need it natively. Kept small and pinned by golden vectors in `contracts/src/colca_data_contracts/vectors/`, read by the tests on both sides. |
| Projection | the key-value view, historian tables | Read-side views of a definition. They never re-validate and never redefine. |

### 3. Configuration is intent, runtime state is earned

A node's configuration file and the schema bundle it enforces describe what
the node is meant to be. Both are versioned files; the bundle is pinned by its
content digest, and generating it again from the same contracts gives the same
bytes. Offsets, cursors, keys, enrolled identities and metrics are earned while
the node runs. They are never written back into configuration.

### 4. Domain knowledge lives in exactly one package

Topic grammar, contract names and classes, the grant grammar, identity kinds,
the element namespace and validation live in `plugins/uns` and nowhere else.
The package depends on the standard library only, so it cannot reach back into
the infrastructure, and domain rules cannot scatter.

Where the domain needs something from the infrastructure, `plugins/uns`
declares the interface and the core implements it (`EntityStore`, `Bindings`,
`Placements`, `Namespace`, `Scope`).

The core asks the domain questions and does not switch on its vocabulary.
`uns.IsState(class)` and `entry.IsDraining()` are fine;
`if class == uns.ClassCmd { … }` inside `internal/` is not, because the rule
about commands would then live in two packages. When a new decision is needed,
add a predicate to `plugins/uns`.

Enforced by `plugins/uns/arch_test.go`: one test holds the standard-library
ceiling, the other walks the core's syntax tree and rejects comparisons with
the domain's constants.

### 5. Test with purpose, at the right level

Every test pins a behaviour at one level:

- package tests for a single component,
- multi-node tests in one process (`tests/`, the bench harness) for replication,
  authorization across the tree and recovery,
- the container smoke test (`demo/smoke.sh`) for the image, the doors and the
  demo topology,
- golden vectors for rules that two languages implement.

A feature brings tests for the ways it can realistically fail. Tests that
restate a constant or repeat what another level already pins are removed.
Every level stays runnable with one command.

### 6. Everything is a node

Everything that takes part in a node's world — a child node, a local service,
an external service — is placed the same way: bound to at most one system
element, and that binding is both its position and its authority. There are no
participant types with rules of their own.

- **Position is authority.** What a participant may write is the subtree it is
  bound to. Moving the element changes what it may do, live.
- **Unplaced is a position.** A participant without an element is bound to the
  node itself. Where an absent element must not read as unlimited scope, the
  code refuses it explicitly at that site (`Ancestry.Covers`).
- **Ownership resolves to nodes.** Topic level 4 is the node that owns the
  data, the same for every publisher at that node. A participant's identity
  decides whether it may write; it never appears in the topic.
- **The one-clause test.** Local and external services differ in one clause: a
  local binding carries an implicit write, an external one does not.

### 7. Enforce principles with tests

A principle ships with a mechanical check wherever one is possible: the
architecture tests in `plugins/uns/arch_test.go`, the golden vectors, the
bundle determinism and parity tests, the benchmark gate, and
`scripts/check-no-working-notes.sh`.

A new enforcement test is mutation-checked before it is trusted: break what it
guards and watch it fail. A check that cannot fail is worse than none, because
it is believed.
