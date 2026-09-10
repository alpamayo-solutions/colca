# colca-data-contracts

The payload contracts that Colca nodes and the services around them agree on,
written as Python dataclasses on top of [franzmq](https://pypi.org/project/franzmq/).

The package contains

- one class per contract (`_Metric`, `_Signal`, `_SystemElement`, `_CmdParam`,
  …) with encoding, decoding and validation,
- topic helpers that put the publishing node and the configured topic root
  into every topic (`node_topic`, `COLCA_TOPIC_ROOT`),
- the builtin data models, semantic tags and metadata types,
- the generator for the schema bundle that nodes enforce
  (`scripts/generate_bundle.py`),
- golden vectors in `src/colca_data_contracts/vectors/`, which the Go tests of
  this repository read as well, so both sides answer the same cases.

## Use

```bash
pip install ./contracts
```

```python
from colca_data_contracts import node_topic
from colca_data_contracts.payload import Metric

topic = node_topic(Metric, "line1", "press3", "temp")   # colca/v1/_Metric/<NODE_ID>/line1/press3/temp
```

`NODE_ID` names the node the process publishes under; `COLCA_TOPIC_ROOT`
changes the first segment from `colca`.

## Develop

```bash
make contracts-test     # from the repository root
make bundle             # writes build/bundle/contracts-bundle.json
```
