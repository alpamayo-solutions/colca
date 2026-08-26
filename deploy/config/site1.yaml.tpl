# deploy/config/site1.yaml.tpl — level 2: parent global, two children.
#
# Like the global node it carries an MQTT listener with a read-only `observer`
# client: the site bus mirrors everything both edges replicate up.
ulid: n-site1
data_dir: /data
secrets_dir: /secrets
log_level: debug
key_file: /keys/site1.key
api: { addr: ":443", local_addr: ":80", token: "demo-admin-token" }
mqtt: { addr: ":8883" }
mqtt_local: { addr: ":1883" }
repl: { addr: ":9443" }
parent:
  url: https://global:9443
  pubkey: ${PUB_GLOBAL}
