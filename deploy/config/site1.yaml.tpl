# deploy/config/site1.yaml.tpl — level 2: parent global, two children, no MQTT.
ulid: n-site1
data_dir: /data
log_level: debug
key_file: /keys/site1.key
api: { addr: ":8080", token: "demo-admin-token" }
repl: { addr: ":9443" }
parent:
  url: https://global:9443
  pubkey: ${PUB_GLOBAL}
children:
  - { ulid: n-edge1, pubkey: ${PUB_EDGE1}, mount: edge1 }
  - { ulid: n-edge2, pubkey: ${PUB_EDGE2}, mount: edge2 }
