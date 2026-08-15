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
