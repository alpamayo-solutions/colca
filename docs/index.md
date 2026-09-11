# Colca

![A Colca tree](img/tree.svg)

Colca is an MQTT broker for plants that are organised as a tree: machines at
the edge, a hall or a site above them, perhaps a company on top. Every level
runs the same program, `colcad`.

A node stores every record it accepts before anyone sees it, keeps the current
value of every path, and replicates to its parent over one connection it opens
itself. Commands travel down the same tree and are acknowledged back up.

- **Offline is normal.** An edge keeps accepting data and executing commands
  while its uplink is gone, and sends everything in order once the link is back.
- **One namespace for the plant.** Subscribe to `colca/#` at the top and you see
  every machine, with each level's position in the topic.
- **State and history from one store.** Retained MQTT messages, a key-value view
  over HTTP and replayable streams with named cursors always agree.
- **No certificate authority.** Nodes and machines are identified by ed25519
  keys that are enrolled once. People sign in with a token from any OIDC
  provider.

## Where to start

- [Try the demo](get-started/demo.md): a tree of four nodes and two machines in
  containers.
- [Run a node](get-started/single-node.md) on your own machine.
- [How Colca works](concepts.md) is the best first read on the ideas.
- [chaski](chaski/index.md) is the Python SDK for services that run next to a
  node.

Colca is licensed under the
[Functional Source License, Version 1.1, ALv2 Future License](https://github.com/alpamayo-solutions/colca/blob/main/LICENSE.md).
