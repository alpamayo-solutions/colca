# colca/Makefile
.PHONY: test build docker demo world world-down smoke ci bench bench-scenarios bench-check
test:
	go test ./... -race -count=1 -v
build:
	CGO_ENABLED=0 go build -o bin/colcad ./cmd/colcad
	CGO_ENABLED=0 go build -o bin/colca-keygen ./cmd/colca-keygen
	CGO_ENABLED=0 go build -o bin/colca-machine ./cmd/colca-machine
	CGO_ENABLED=0 go build -o bin/colca-grantsync ./cmd/colca-grantsync
docker:
	cd .. && uv run scripts/dev.py bundle --context
	docker build -f deploy/Dockerfile -t colca:dev \
		--build-context contracts-bundle=../test-results/bundle-ctx .
# demo runs demo/demo.sh, which executes demo/smoke.sh with narration. That
# script is the human walkthrough only — the AUTHORITATIVE CI gate for these
# assertions is the pytest system suite (tests/system, smoke marker), which
# `smoke` below delegates to. Do not let smoke.sh's assertions drift from it.
demo: docker
	bash demo/demo.sh
# An interactive world that STAYS up (the level-4 topology, enrolled and
# seeded): see docs/source/development/testing.rst.
world:
	cd .. && uv run scripts/dev.py world up
world-down:
	cd .. && uv run scripts/dev.py world down
smoke: docker
	cd .. && COLCA_IMAGE=colca:dev uv run scripts/dev.py test system -m smoke
ci: test docker smoke
bench:
	go test ./internal/store/ -run '^$$' -bench . -benchtime 2s
bench-scenarios: build
	go run ./cmd/colca-bench all --colcad bin/colcad
bench-check:
	go run ./cmd/colca-bench check --thresholds bench/thresholds.json
