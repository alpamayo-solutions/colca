#!/usr/bin/env bash
# The Go test shards CI runs side by side. This file is the only place they are
# defined: the workflow takes its matrix from `matrix` and each shard's packages
# from `packages`.
#
#   scripts/go-test-shards.sh matrix        the shard names as a JSON list
#   scripts/go-test-shards.sh packages NAME the import paths one shard tests
#   scripts/go-test-shards.sh check         fail unless the shards cover every
#                                           package of `go list ./...` exactly once
#
# Each named shard lists package patterns; `rest` takes every package the named
# shards leave out. Split a shard when it becomes the slowest job of the run.
set -euo pipefail

cd "$(dirname "$0")/.."

SHARDS="bench tests http-mqtt rest"

patterns() {
  case "$1" in
    bench) echo ./bench/... ;;
    tests) echo ./tests/... ;;
    http-mqtt) echo ./internal/httpapi/... ./internal/mqttsrv/... ;;
    *) return 1 ;;
  esac
}

# -find names the packages without loading their dependencies, so no module is downloaded.
list() {
  go list -find "$@" | LC_ALL=C sort
}

named() {
  local shard
  for shard in $SHARDS; do
    if [ "$shard" != rest ]; then
      # shellcheck disable=SC2046  # the patterns are meant to split
      list $(patterns "$shard")
    fi
  done
}

packages() {
  case "$1" in
    rest) LC_ALL=C comm -23 <(list ./...) <(named | LC_ALL=C sort -u) ;;
    *)
      if ! pats=$(patterns "$1"); then
        echo "::error::unknown Go test shard '$1'; the shards are: $SHARDS" >&2
        exit 2
      fi
      # shellcheck disable=SC2086  # the patterns are meant to split
      list $pats
      ;;
  esac
}

check() {
  local all union dup shard n failed=0
  all=$(list ./...)
  union=""
  for shard in $SHARDS; do
    n=$(packages "$shard" | grep -c . || true)
    if [ "$n" -eq 0 ]; then
      echo "::error::Go test shard '$shard' has no packages; remove it or fix its patterns" >&2
      failed=1
    fi
    union+=$(packages "$shard")$'\n'
  done
  union=$(grep . <<< "$union" | LC_ALL=C sort)
  dup=$(uniq -d <<< "$union")
  if [ -n "$dup" ]; then
    echo "::error::packages in more than one Go test shard:" >&2
    echo "$dup" >&2
    failed=1
  fi
  if [ "$(LC_ALL=C sort -u <<< "$union")" != "$all" ]; then
    echo "::error::the Go test shards do not cover go list ./... exactly:" >&2
    diff <(echo "$all") <(LC_ALL=C sort -u <<< "$union") >&2 || true
    failed=1
  fi
  if [ "$failed" -ne 0 ]; then
    exit 1
  fi
  echo "$(grep -c . <<< "$all") packages in $(wc -w <<< "$SHARDS" | tr -d ' ') shards, each exactly once"
}

case "${1:-}" in
  matrix)
    printf '['
    sep=""
    for shard in $SHARDS; do
      printf '%s"%s"' "$sep" "$shard"
      sep=","
    done
    printf ']\n'
    ;;
  packages)
    [ $# -eq 2 ] || { echo "usage: $0 packages NAME" >&2; exit 2; }
    packages "$2"
    ;;
  check) check ;;
  *)
    echo "usage: $0 matrix | packages NAME | check" >&2
    exit 2
    ;;
esac
