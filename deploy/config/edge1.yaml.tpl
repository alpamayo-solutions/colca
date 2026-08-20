# deploy/config/edge1.yaml.tpl — level 3: parent site1, MQTT for machine m1.
ulid: n-edge1
data_dir: /data
log_level: debug
key_file: /keys/edge1.key
api: { addr: ":443", local_addr: ":80", token: "demo-admin-token" }
mqtt: { addr: ":8883" }
mqtt_local: { addr: ":1883" }
repl: { addr: ":9443" }
auth:
  issuer: http://keycloak:8080/realms/colca
  audience: colca
  jwks_url: http://keycloak:8080/realms/colca/protocol/openid-connect/certs
parent:
  url: https://site1:9443
  pubkey: ${PUB_SITE1}
