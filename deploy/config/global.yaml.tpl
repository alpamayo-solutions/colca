# deploy/config/global.yaml.tpl — level 1: no parent, one child (site1).
#
# The hub runs an MQTT listener even though no machine is attached to it: every
# record appended to this node's stream — including everything replicated up
# from the whole tree — is mirrored onto that bus under its stored topic. A
# subscriber here sees the entire plant live. `observer` is a read-only client
# (no mount, so the engine refuses everything it publishes).
ulid: n-global
data_dir: /data
log_level: debug
key_file: /keys/global.key
api: { addr: ":8080", token: "demo-admin-token" }
mqtt: { addr: ":1883" }
repl: { addr: ":9443" }
