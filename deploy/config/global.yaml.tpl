# deploy/config/global.yaml.tpl — level 1: no parent, no MQTT, one child (site1).
ulid: n-global
data_dir: /data
log_level: debug
key_file: /keys/global.key
api: { addr: ":8080", token: "demo-admin-token" }
repl: { addr: ":9443" }
children:
  - { ulid: n-site1, pubkey: ${PUB_SITE1}, mount: site1 }
