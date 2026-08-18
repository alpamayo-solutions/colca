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
mqtt: { addr: ":8883" }
# Human doors (JWT): live only when the human-auth compose profile runs the
# keycloak container; otherwise the verifier retries its JWKS fetch and the
# doors reject every token (machines are unaffected).
mqtt_human: { tcp_addr: ":8884", ws_addr: ":8885" }
auth:
  issuer: http://keycloak:8080/realms/colca
  audience: colca
  jwks_url: http://keycloak:8080/realms/colca/protocol/openid-connect/certs
repl: { addr: ":9443" }
