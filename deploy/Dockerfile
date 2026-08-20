# colca/deploy/Dockerfile — build context is colca/ (see Makefile: docker build -f deploy/Dockerfile .)
#
# Both base images are public and pullable anonymously; there is no registry
# login anywhere in this repo's build path.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# All five binaries live in one image. colcad runs a node; the rest are
# SIBLINGS of it, never part of it, each for the same reason — what they talk to
# stays out of the node core and off the message path:
#   colca-grantsync   carries grants between Keycloak and the tree
#   colca-historian   carries metrics into the Timescale hypertables
#   colca-machine     the demo machine simulator
#   colca-keygen      in-container key setup
RUN CGO_ENABLED=0 go build -trimpath -o /out/colcad ./cmd/colcad \
 && CGO_ENABLED=0 go build -trimpath -o /out/colca-grantsync ./cmd/colca-grantsync \
 && CGO_ENABLED=0 go build -trimpath -o /out/colca-historian ./cmd/colca-historian \
 && CGO_ENABLED=0 go build -trimpath -o /out/colca-machine ./cmd/colca-machine \
 && CGO_ENABLED=0 go build -trimpath -o /out/colca-keygen ./cmd/colca-keygen \
 && CGO_ENABLED=0 go build -trimpath -o /out/colca-service ./cmd/colca-service

# Pinned by digest, not by tag: distroless publishes no semver tags for
# static-debian12 — only latest/nonroot/debug — and hadolint rejects both an
# untagged base (DL3006) and :latest (DL3007). This digest is what :latest
# resolved to on 2026-08-14, so runtime behaviour is unchanged.
FROM gcr.io/distroless/static-debian12@sha256:6447365a6337c3732f412d1b74357b30a633831955b2bc45552b0086be907687
COPY --from=build /out/ /usr/local/bin/
# The contracts bundle generated from the SAME commit (schema-bundle design
# §6.1 channel 1): colcad loads it from this baked default path unless the
# config names another bundle. The named context is supplied by every build
# caller (bake target `colca`, colca/Makefile, dev.py) after running
# `uv run scripts/dev.py bundle --context`.
# contracts-bundle is a buildx NAMED CONTEXT (docker-bake.hcl), not a stage.
# hadolint ignore=DL3022
COPY --from=contracts-bundle contracts-bundle.json /etc/colca/contracts-bundle.json
ENTRYPOINT ["/usr/local/bin/colcad"]
