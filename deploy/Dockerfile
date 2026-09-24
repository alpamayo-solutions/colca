# The colca image. Build it from the repository root with `make docker`, which
# generates the contracts bundle first.
#
# The build stage cross-compiles on the build host, so a multi-platform build
# needs no emulation. colcad is the entrypoint; the other binaries run beside a
# node.
FROM --platform=$BUILDPLATFORM golang:1.26.8-alpine@sha256:8ac98ca534ac3f51e1f420a1dd2c15e74c75cfa0f23f3ad27eb5d7236c349a0c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN for cmd in colcad colca-grantsync colca-historian colca-machine colca-keygen \
      colca-healthcheck colca-service colca-volume-init; do \
      CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
        go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o "/out/$cmd" "./cmd/$cmd" || exit 1; \
    done \
 && install -d -m 0750 -o 65532 -g 65532 /runtime/data /runtime/secrets /runtime/keys

# distroless has no version tags for static-debian12, so the base is pinned by digest.
FROM gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2
COPY --from=build /out/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /runtime/ /
# The contracts bundle from the same commit. colcad uses it unless its config names another.
# hadolint ignore=DL3022
COPY --from=contracts-bundle contracts-bundle.json /etc/colca/contracts-bundle.json
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/colcad"]
