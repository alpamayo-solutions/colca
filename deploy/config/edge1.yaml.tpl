# deploy/config/edge1.yaml.tpl — level 3: parent site1, MQTT for machine m1.
ulid: n-edge1
data_dir: /data
secrets_dir: /secrets
log_level: debug
key_file: /keys/edge1.key
api: { addr: ":443", local_addr: ":80", token: "demo-admin-token" }
mqtt: { addr: ":8883" }
mqtt_local: { addr: ":1883" }
# A human door on a LEAF, so a grant authored at the root can be watched being
# enforced two hops down rather than only where it was written. Same profile
# caveat as global.yaml.tpl's doors: with no keycloak container the verifier
# retries its JWKS fetch and this door rejects every token; machines are
# unaffected. No ws_addr — the browser transport is only exercised at the hub.
mqtt_human: { tcp_addr: ":8884" }
repl: { addr: ":9443" }
auth:
  issuers:
    - url: http://keycloak:8080/realms/colca
  audience: colca
  jwks_url: http://keycloak:8080/realms/colca/protocol/openid-connect/certs
parent:
  url: https://site1:9443
  pubkey: ${PUB_SITE1}
