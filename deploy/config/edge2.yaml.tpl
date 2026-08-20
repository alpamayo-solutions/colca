# deploy/config/edge2.yaml.tpl — level 3: parent site1, MQTT for machine m2.
ulid: n-edge2
data_dir: /data
log_level: debug
key_file: /keys/edge2.key
api: { addr: ":443", local_addr: ":80", token: "demo-admin-token" }
mqtt: { addr: ":8883" }
mqtt_local: { addr: ":1883" }
repl: { addr: ":9443" }
parent:
  url: https://site1:9443
  pubkey: ${PUB_SITE1}
