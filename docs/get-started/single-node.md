# Run a node

Build the binaries, create a key and start a node with a minimal configuration:

```bash
make build
bin/colca-keygen ./node.key        # writes the key and prints its public half

cat > node.yaml <<'EOF'
ulid: n-edge1
data_dir: ./data
key_file: ./node.key
api: { local_addr: "127.0.0.1:8080" }
EOF

bin/colcad node.yaml
```

A service on the same machine can now publish through the local door and read
the value back:

```bash
curl -s -H 'X-Colca-Service: press-bridge' -H 'Content-Type: application/json' \
  -d '{"topic":"colca/v1/_Metric/n-edge1/line1/press3/temp","payload":{"v":71.5}}' \
  http://127.0.0.1:8080/publish

curl -s -H 'X-Colca-Service: press-bridge' 'http://127.0.0.1:8080/kv?prefix=line1'
```

The local door has no credential, which is why it listens on `127.0.0.1` here.
[Configuration](../configuration.md) shows a tree with a parent, TLS doors and
enrolled machines.
