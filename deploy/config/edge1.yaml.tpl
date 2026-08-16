# deploy/config/edge1.yaml.tpl — level 3: parent site1, MQTT for machine m1.
ulid: n-edge1
data_dir: /data
log_level: debug
key_file: /keys/edge1.key
api: { addr: ":8080", token: "demo-admin-token" }
mqtt: { addr: ":1883" }
repl: { addr: ":9443" }
parent:
  url: https://site1:9443
  pubkey: ${PUB_SITE1}
# beacon_interval shortened from the 10s (design §2.5) default: the
# session-establishment beacon (OnSessionEstablished, mqttsrv.go) races a
# reconnecting machine's own SUBSCRIBE to colca/v1/_TimeSync/+ — confirmed via
# tests/system's skew/reconnect-hold chaos scenarios that this specific
# machine's OWN first beacon is not reliably delivered to itself within a
# real Docker reconnect's timing, even though an already-subscribed
# bystander (an observer, or a different machine) gets it fine. A 3s
# periodic cadence bounds the worst-case wait for m1's first synced offset
# to comfortably inside the default 10s hold_ms, independent of whether the
# connect-triggered beacon specifically lands — see
# the time sync move drain design §2.2 [delta]
# for the open question of whether the connect-triggered beacon's delivery
# race itself needs a production fix.
time_sync:
  beacon_interval: 3s
