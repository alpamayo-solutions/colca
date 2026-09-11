# Contributing

Bug reports, questions and pull requests are welcome. This page says how to
build and test the project and what a change needs before it can be merged.
Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Build and test

You need Go 1.26, [uv](https://docs.astral.sh/uv/) for the Python side, and
Docker for the end-to-end demo.

```bash
make test            # Go unit and in-process integration tests, with -race
make contracts-test  # the data contracts package (Python)
make check           # gofmt, go vet, and the repository hygiene checks
make lint            # golangci-lint, govulncheck, ruff, bandit, mypy, hadolint, shellcheck, actionlint, yamllint
make smoke           # builds the image, starts a four-node tree, asserts on it
```

`make test` is what most changes need. Run `make smoke` when you touch
replication, the doors, or anything the demo topology exercises. CI runs all of
them on every pull request; `make lint` needs Docker for hadolint and gitleaks.

## Git hooks

The repository ships [pre-commit](https://pre-commit.com) hooks: gitleaks,
formatting and the fast linters on every commit, golangci-lint and mypy before
a push, and a check that the commit subject follows the convention below.

```bash
uv tool install pre-commit
pre-commit install
```

## Changing a contract

A contract has two sides: its shape in `contracts/` (Python) and the rules the
node applies in Go. Where both sides must agree, a golden vector in
`contracts/src/colca_data_contracts/vectors/` pins the answer and both test
suites read it. Change the vector, then make both suites pass.

## Pull requests

- Keep a pull request to one change. Split a refactoring from the behaviour
  change it enables.
- Add or adjust a test that fails without your change.
- Write the commit subject as `type(scope): what changes`, for example
  `fix(repl): hold the downlink cursor when a command cannot be persisted`.
  Types are `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`
  and `chore`; the scope is optional.
- Explain in the pull request why the change is needed. The diff shows what.

## Keep working notes out of the repository

Plans, design drafts, task lists and instruction files for coding assistants
(`CLAUDE.md`, `AGENTS.md`, `.cursor/`, `docs/plans/` and the like) do not
belong in commits. Keep them in the issue tracker or in your local checkout.
CI rejects them; `scripts/check-no-working-notes.sh` runs the same check
locally. Code comments explain the code as it is, not the history of how it
was planned.

## Releases

[release-please](https://github.com/googleapis/release-please) keeps a release
pull request open that proposes the next version and the changelog, both taken
from the commit subjects: `fix` bumps the patch version, `feat` and breaking
changes bump the minor version. The major version never changes on its own;
1.0 and later are set by hand. Merging that pull request tags the release, and
CI then publishes the image to `ghcr.io/alpamayo-solutions/colca` and attaches
the `colca-data-contracts` packages, the `colcad` wheels, SBOMs and checksums
to the GitHub release. Every push to `main` also publishes the image as `main`
and `sha-<commit>`.

## Contributor License Agreement

Unless you belong to the organisation that owns this repository, tick the box
"I agree to the Contributor License Agreement" in each pull request's
description; a check blocks the merge until it is ticked. The agreement is in
[CLA.md](CLA.md). For larger contributions, or when you contribute for your
employer, we may also ask for a signed copy by email. You keep the copyright in
your work.

The project is licensed under the Functional Source License with an Apache 2.0
future grant (see [LICENSE.md](LICENSE.md)). The agreement lets the licensor
keep that promise for contributed code as well.
