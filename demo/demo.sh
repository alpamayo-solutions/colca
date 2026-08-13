#!/usr/bin/env bash
# colca/demo/demo.sh — the human-facing walkthrough of the 4-node topology.
#
# It runs demo/smoke.sh with narration enabled, so it executes byte-for-byte the
# same assertions with the same timeouts and fails just as loudly. Narration only
# adds output (the KV state of all four nodes side by side, m1's log around the
# command roundtrip, explicit down/buffering/replay markers) — a demo that could
# silently drop a check would be worse than no demo.
set -euo pipefail

cat <<'BANNER'
════════════════════════════════════════════════════════════════════════════
 Colca demo — one global node, one site, two edges, two machines

   global (level 1)                 mounts: site1 → n-site1
      ▲ mTLS
   site1  (level 2)                 mounts: edge1 → n-edge1, edge2 → n-edge2
      ▲ mTLS      ▲ mTLS
   edge1         edge2              clients: m1 → machine m1, m2 → machine m2
      ▲ MQTT        ▲ MQTT
     m1            m2

 Three things are proven, in this order:
   1) a metric published by a machine reaches global with the full mount path
   2) a command issued at global reaches the machine and its ack comes back
   3) an edge buffers while its parent is down and replays when it returns
════════════════════════════════════════════════════════════════════════════
BANNER

COLCA_NARRATE=1 exec bash "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/smoke.sh" "$@"
