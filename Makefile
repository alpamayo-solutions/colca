# colca/Makefile
.PHONY: test build docker demo smoke ci
test:
	go test ./... -race -count=1 -v
build:
	CGO_ENABLED=0 go build -o bin/colcad ./cmd/colcad
	CGO_ENABLED=0 go build -o bin/colca-keygen ./cmd/colca-keygen
	CGO_ENABLED=0 go build -o bin/colca-machine ./cmd/colca-machine
docker:
	docker build -f deploy/Dockerfile -t colca:dev .
demo: docker
	bash demo/demo.sh
smoke: docker
	bash demo/smoke.sh
ci: test docker smoke
