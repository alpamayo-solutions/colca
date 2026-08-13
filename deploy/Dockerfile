# colca/deploy/Dockerfile — build context is colca/ (see Makefile: docker build -f deploy/Dockerfile .)
#
# Both base images are public and pullable anonymously; there is no registry
# login anywhere in this repo's build path.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# All three binaries live in one image: colcad runs a node, colca-machine is the
# demo machine simulator, colca-keygen is available for in-container key setup.
RUN CGO_ENABLED=0 go build -trimpath -o /out/colcad ./cmd/colcad \
 && CGO_ENABLED=0 go build -trimpath -o /out/colca-machine ./cmd/colca-machine \
 && CGO_ENABLED=0 go build -trimpath -o /out/colca-keygen ./cmd/colca-keygen

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/colcad"]
