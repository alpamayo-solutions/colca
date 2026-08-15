#!/usr/bin/env bash
# colca/deploy/gen-keys.sh — generates the four node keys and renders the config
# templates with the resulting public keys.
#
# Idempotent: an existing key pair is NEVER regenerated, because every node's
# config pins its peers by public key — a new key would silently break the
# topology that is already on disk (and in the data volumes). Safe to run from
# any working directory.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NODES="global site1 edge1 edge2"
MACHINES="m1 m2"

mkdir -p keys
for n in $NODES; do
  # A key without its .pub is unusable (the pubkey cannot be recovered by the
  # keygen tool), so the pair is regenerated only when it is incomplete.
  if [ ! -f "keys/$n.key" ] || [ ! -s "keys/$n.pub" ]; then
    rm -f "keys/$n.key" "keys/$n.pub"
    go run ../cmd/colca-keygen "keys/$n.key" > "keys/$n.pub"
    echo "gen-keys: generated keys/$n.key"
  fi
  chmod 600 "keys/$n.key"
done

# Machine keys: a machine's key IS its credential — generated once here,
# enrolled at its node by smoke.sh through the enrollment door.
for m in $MACHINES; do
  if [ ! -f "keys/$m-machine.key" ] || [ ! -s "keys/$m-machine.pub" ]; then
    rm -f "keys/$m-machine.key" "keys/$m-machine.pub"
    go run ../cmd/colca-keygen "keys/$m-machine.key" > "keys/$m-machine.pub"
    echo "gen-keys: generated keys/$m-machine.key"
  fi
  chmod 644 "keys/$m-machine.key"
done

# Observer key for bus-tapping clients (system suite, humans): -cert writes
# the PEM client certificate non-Go MQTT clients present at the TLS door.
if [ ! -f "keys/observer.key" ] || [ ! -s "keys/observer.pub" ]; then
  rm -f "keys/observer.key" "keys/observer.pub" "keys/observer.key.crt"
  go run ../cmd/colca-keygen -cert "keys/observer.key" > "keys/observer.pub"
  echo "gen-keys: generated keys/observer.key (+cert)"
fi
chmod 644 "keys/observer.key"

{
  echo "PUB_GLOBAL=$(cat keys/global.pub)"
  echo "PUB_SITE1=$(cat keys/site1.pub)"
  echo "PUB_EDGE1=$(cat keys/edge1.pub)"
  echo "PUB_EDGE2=$(cat keys/edge2.pub)"
} > pubkeys.env

set -a
# shellcheck source=/dev/null
. ./pubkeys.env
set +a

# envsubst (gettext) is the nice path; CI images often lack it, so fall back to a
# sed loop over exactly the four variables. Nothing is ever installed here.
if command -v envsubst >/dev/null 2>&1; then
  for n in $NODES; do
    envsubst < "config/$n.yaml.tpl" > "config/$n.yaml"
  done
else
  for n in $NODES; do
    sed -e "s|\${PUB_GLOBAL}|${PUB_GLOBAL}|g" \
        -e "s|\${PUB_SITE1}|${PUB_SITE1}|g" \
        -e "s|\${PUB_EDGE1}|${PUB_EDGE1}|g" \
        -e "s|\${PUB_EDGE2}|${PUB_EDGE2}|g" \
        "config/$n.yaml.tpl" > "config/$n.yaml"
  done
fi

# A config that still carries a placeholder would start a node that can never
# authenticate its peers — fail loudly instead of debugging it later in TLS logs.
for n in $NODES; do
  # shellcheck disable=SC2016  # the pattern is a literal ${PUB_, not an expansion
  if grep -q '\${PUB_' "config/$n.yaml"; then
    echo "gen-keys: FAILED — config/$n.yaml still contains an unsubstituted placeholder" >&2
    exit 1
  fi
done

cat pubkeys.env
echo "gen-keys: configs rendered (config/{global,site1,edge1,edge2}.yaml)"
