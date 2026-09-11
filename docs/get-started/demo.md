# Try the demo

The demo runs a small tree in containers: a global node, a site below it, two
edges below the site, and a simulated machine on each edge.

## What you need

- Go 1.26
- Docker with Compose v2
- [uv](https://docs.astral.sh/uv/) and python3
- a curl built with OpenSSL

The curl that ships with macOS cannot complete a TLS handshake with the nodes'
ed25519 certificates. Install curl with Homebrew and put
`$(brew --prefix curl)/bin` first on your `PATH`.

## Run it

From a checkout of the [repository](https://github.com/alpamayo-solutions/colca):

```bash
make smoke    # builds the image, starts the tree, checks it, cleans up
make demo     # the same, explained as it goes
```

The smoke run checks three things:

1. Metrics from both machines reach the global node with their full paths.
2. A command sent at the global node travels down to a machine, and the
   acknowledgement comes back up.
3. While the site node is stopped, the machines keep publishing; once it is
   back, the tree catches up.
