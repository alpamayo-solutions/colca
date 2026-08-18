#!/usr/bin/env bash
# colca/demo/smoke.sh — end-to-end assertion of the 4-node / 3-level topology in
# real containers. Exit 0 means: uplink with mount rewriting, command/ack
# roundtrip through two downlink hops, and offline buffering + replay all work.
#
# This is also the body of demo/demo.sh: that script runs THIS file with
# COLCA_NARRATE=1, so the human walkthrough and the CI assertion can never drift
# apart. Narration only ever adds output — never a check, never a timeout.
#
# Safe to re-run: the stack is torn down (`down -v --remove-orphans`) before it
# is brought up, and again on every exit path via trap.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../deploy"

TOK="X-Colca-Token: demo-admin-token"
# The API is TLS with the node's self-signed key (pinning model, no CA): -k.
G=https://127.0.0.1:18080
S1=https://127.0.0.1:18081
E1=https://127.0.0.1:18082
E2=https://127.0.0.1:18083
CMD_TOPIC="colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed"
ACK_TOPIC="colca/v1/_Ack/m1/site1/edge1/m1/set-speed"

narrating() { [ "${COLCA_NARRATE:-0}" = "1" ]; }
say() { if narrating; then printf '%s\n' "$*"; fi; }
fail() {
  echo >&2
  echo "FAIL: $*" >&2
  exit 1
}

for tool in docker curl python3; do
  command -v "$tool" >/dev/null 2>&1 ||
    fail "'$tool' is required but not on PATH (python3 parses the JSON responses)"
done
docker compose version >/dev/null 2>&1 || fail "'docker compose' (v2) is required but not available"

# json_int <url-path> <python-expression-over-d> → prints the value, or nothing
# when the node is not answering yet (the callers poll, so that is not an error).
json_int() {
  curl -skf -H "$TOK" "$1" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print($2)" 2>/dev/null || true
}

kv_dump() { # $1 = label, $2 = base url
  echo "   ┌─ KV @ $1"
  curl -skf -H "$TOK" "$2/kv" 2>/dev/null | python3 -c '
import json, sys
entries = sorted(json.load(sys.stdin)["entries"], key=lambda e: e["path"])
if not entries:
    print("   │  (empty)")
for e in entries:
    print("   │  %-24s node=%-4s topic=%s" % (e["path"], e["node_id"], e["topic"]))
' || echo "   │  (unreachable)"
  echo "   └─"
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo
    echo "──────── diagnostics (exit $rc) ────────"
    docker compose ps || true
    docker compose logs --tail 50 || true
    echo "────────────────────────────────────────"
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$rc"
}

echo "── generating node keys and rendering configs…"
bash gen-keys.sh
say "   (keys are pinned per node: nothing trusts a CA, everything trusts a key)"

echo "── starting the 4-node topology (global ← site1 ← edge1/edge2, machines m1/m2)…"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
trap cleanup EXIT
docker compose up -d
if narrating; then
  echo
  docker compose ps
  echo
fi

echo "── waiting for global health…"
ok=0
for _ in $(seq 1 60); do
  if curl -skf "$G/healthz" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 1
done
[ "$ok" = 1 ] || fail "global never became healthy on $G/healthz"
say "   global answers /healthz: $(curl -skf "$G/healthz")"

echo "── enrolling the tree: children at their parents, machines at their edges…"
say "   the registry is runtime state: an identity exists at a node only after"
say "   POST /enroll (entry-before-connect); everything below retries until then."
# An identity binds to a system element, not to a path (id-grants design §4),
# so the element is authored first and the entry names it. The mount is then
# wherever that element sits, now and after any later rename.
place() { # $1 = base url, $2 = node ulid, $3 = path -> echoes the element id
  element="el-$(echo "$3" | tr '/' '-')"
  ok=0
  for _ in $(seq 1 60); do
    if curl -skf -H "$TOK" -H "Content-Type: application/json" -X POST "$1/publish" \
      -d "{\"topic\":\"colca/v1/_SystemElement/$2/$3\",\"payload\":{\"id\":\"$element\",\"name\":\"$3\"}}" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 1
  done
  [ "$ok" = 1 ] || fail "placing element at $3 on $1 failed"
  echo "$element"
}
enroll() { # $1 = base url, $2 = ulid, $3 = kind, $4 = mount, $5 = pubkey file, $6 = node ulid
  element=$(place "$1" "$6" "$4")
  ok=0
  for _ in $(seq 1 60); do
    if curl -skf -H "$TOK" -H "Content-Type: application/json" -X POST "$1/enroll" \
      -d "{\"ulid\":\"$2\",\"kind\":\"$3\",\"element\":\"$element\",\"pubkey\":\"$(cat "$5")\"}" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 1
  done
  [ "$ok" = 1 ] || fail "enrolling $2 at $1 failed"
  say "   enrolled $2 ($3) at $1 on element $element, which sits at '$4'"
}
# Groups are definitions, authored once at the root and descending to every
# node below it (definition-stream design §8). Keycloak carries who is in which
# group; what a group MAY do lives here.
define_group() { # $1 = base url, $2 = node ulid, $3 = group id, $4 = grants JSON array
  ok=0
  for _ in $(seq 1 60); do
    if curl -skf -H "$TOK" -H "Content-Type: application/json" -X POST "$1/publish" \
      -d "{\"topic\":\"colca/v1/_Group/$2/$3\",\"payload\":{\"id\":\"$3\",\"name\":\"$3\",\"grants\":$4}}" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 1
  done
  [ "$ok" = 1 ] || fail "defining group $3 at $1 failed"
  say "   defined group $3 with grants $4"
}

enroll "$G" n-site1 node site1 keys/site1.pub n-global
enroll "$S1" n-edge1 node edge1 keys/edge1.pub n-site1
enroll "$S1" n-edge2 node edge2 keys/edge2.pub n-site1
enroll "$E1" m1 machine m1 keys/m1-machine.pub n-edge1
enroll "$E2" m2 machine m2 keys/m2-machine.pub n-edge2

define_group "$G" n-global 01HGRP-SITE1-OPERATORS '["read:el-site1/#","cmd:el-m1/#:param"]'
define_group "$G" n-global 01HGRP-ADMINS '["admin:#","read:#","cmd:#:admin"]'

echo "── 1) uplink: metrics from both machines reach global with full paths"
say "   m1 publishes colca/v1/_Metric/m1/temp to edge1 — global must store it as"
say "   colca/v1/_Metric/m1/site1/edge1/m1/temp (mount inserted at every hop)."
for path in "site1/edge1/m1/temp" "site1/edge2/m2/temp"; do
  ok=0
  n=""
  for _ in $(seq 1 60); do
    n=$(json_int "$G/kv?prefix=$path" 'len(d["entries"])')
    if [ "$n" = "1" ]; then
      ok=1
      echo "   OK: exactly one KV entry at $path"
      break
    fi
    sleep 1
  done
  [ "$ok" = 1 ] || fail "uplink: $path never arrived at global (last entry count: '${n:-none}')"
done
if narrating; then
  echo
  echo "   the same data, seen from every level of the tree:"
  kv_dump "edge1  (level 3)" "$E1"
  kv_dump "edge2  (level 3)" "$E2"
  kv_dump "site1  (level 2)" "$S1"
  kv_dump "global (level 1)" "$G"
  echo
fi

echo "── 2) command roundtrip: global → site1 → edge1 → m1 → ack back at global"
CORR="smoke-$(date +%s)-$$"
EXP=$(($(date +%s) * 1000 + 3600000))
say "   issuing $CMD_TOPIC (correlation_id=$CORR, expires in 1h)"
say "   note: /publish lands on GLOBAL's own bus (127.0.0.1:11880), not on m1's —"
say "   the command only reaches m1 by travelling DOWN the tree, which is exactly"
say "   what is asserted here."
curl -skf -H "$TOK" -H "Content-Type: application/json" -X POST "$G/publish" \
  -d "{\"topic\":\"$CMD_TOPIC\",\"payload\":{\"correlation_id\":\"$CORR\",\"expires_at\":$EXP,\"params\":{\"speed\":7}}}" \
  >/dev/null || fail "publishing the command at global failed"
ok=0
for _ in $(seq 1 60); do
  got=$(curl -skf -H "$TOK" "$G/fetch?stream=commands&cursor=smoke-$CORR&max=500" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(any(r['topic'] == '$ACK_TOPIC'
          and r['payload'].get('correlation_id') == '$CORR'
          and r['payload'].get('result_code') == 200
          for r in d['records']))" 2>/dev/null || true)
  if [ "$got" = "True" ]; then
    ok=1
    echo "   OK: ack $ACK_TOPIC (correlation_id=$CORR, result_code=200)"
    break
  fi
  sleep 1
done
[ "$ok" = 1 ] || fail "ack for $CORR never arrived at global"
if narrating; then
  echo
  echo "   what m1 saw (it receives the mount-stripped colca/v1/_CmdParam/m1/m1/set-speed):"
  docker compose logs --tail 12 m1 || true
  echo
fi

echo "── 3) offline buffering: stop site1, machines keep publishing, restart, catch up"
before=$(json_int "$G/debug/state" 'd["streams"]["metrics"]["next_offset"]')
[ -n "$before" ] || fail "could not read global's metrics next_offset before the outage"
say "   global metrics.next_offset before the outage: $before"
say "   🔌 site1 down — the link between global and both edges is cut"
docker compose stop site1 >/dev/null
say "   📦 edges buffering — m1/m2 keep publishing into edge1/edge2, whose uplink"
say "      cursors stay put because a failed push must never advance them"
sleep 8
if narrating; then
  e1=$(json_int "$E1/debug/state" 'd["streams"]["metrics"]["next_offset"]')
  e2=$(json_int "$E2/debug/state" 'd["streams"]["metrics"]["next_offset"]')
  now=$(json_int "$G/debug/state" 'd["streams"]["metrics"]["next_offset"]')
  echo "   after 8s offline: edge1=$e1  edge2=$e2  global=$now (global frozen at $before)"
  echo "   🔁 replay — starting site1 again"
fi
docker compose start site1 >/dev/null
ok=0
after=""
for _ in $(seq 1 90); do
  after=$(json_int "$G/debug/state" 'd["streams"]["metrics"]["next_offset"]')
  if [ -n "$after" ] && [ "$after" -gt "$((before + 8))" ]; then
    ok=1
    echo "   OK: caught up ($before → $after)"
    break
  fi
  sleep 1
done
[ "$ok" = 1 ] || fail "no catch-up after site1 returned ($before → ${after:-unreadable})"
if narrating; then
  echo
  echo "   the buffered records arrived in one burst, in order, exactly once:"
  kv_dump "global (level 1)" "$G"
  echo
fi

echo "SMOKE PASSED ✔ — full 3-level topology works: uplink, mounts, cmd/ack, offline catch-up"
