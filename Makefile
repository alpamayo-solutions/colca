# Build, test and run Colca. `make help` lists the targets.
GO       ?= go
UV       ?= uv
IMAGE    ?= colca:dev
BUNDLE   := build/bundle/contracts-bundle.json
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BINARIES := colcad colca-keygen colca-machine colca-grantsync colca-historian colca-service \
            colca-healthcheck colca-volume-init colca-bench

.PHONY: help test contracts-test check bundle build docker smoke demo ci wheels \
        bench bench-scenarios bench-check clean

help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

test: ## Go tests, with the race detector
	$(GO) test ./... -race -count=1

contracts-test: ## tests of the Python data contracts
	$(UV) run --project contracts --extra test pytest contracts/tests

check: ## formatting, vet and repository hygiene
	@unformatted="$$(gofmt -l $$(git ls-files '*.go'))"; \
	  if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...
	scripts/check-no-working-notes.sh

bundle: ## the contracts schema bundle for this commit
	@mkdir -p $(dir $(BUNDLE))
	$(UV) run --project contracts python contracts/scripts/generate_bundle.py $(BUNDLE) $(COMMIT)

build: ## all binaries into bin/
	@mkdir -p bin
	@for b in $(BINARIES); do echo "go build $$b"; CGO_ENABLED=0 $(GO) build -trimpath -o bin/$$b ./cmd/$$b || exit 1; done

docker: bundle ## the colca image, with this commit's bundle baked in
	docker build -f deploy/Dockerfile -t $(IMAGE) --build-context contracts-bundle=$(dir $(BUNDLE)) .

smoke: docker ## start the four-node demo tree and assert on it
	COLCA_IMAGE=$(IMAGE) bash demo/smoke.sh

demo: docker ## the same, with narration
	COLCA_IMAGE=$(IMAGE) bash demo/demo.sh

ci: check test contracts-test smoke ## everything CI runs

wheels: ## colcad platform wheels for chaski[node], VERSION=x.y.z
	@test -n "$(VERSION)" || { echo "usage: make wheels VERSION=x.y.z"; exit 2; }
	$(UV) run --no-project --with "setuptools>=77" --with "wheel>=0.42" \
	  python scripts/build_colcad_wheels.py --version $(VERSION) --out build/wheels

bench: ## store micro-benchmarks
	$(GO) test ./internal/store/ -run '^$$' -bench . -benchtime 2s

bench-scenarios: build ## benchmark scenarios against a real edge and hub pair
	$(GO) run ./cmd/colca-bench all --colcad bin/colcad

bench-check: ## compare the newest results with bench/thresholds.json
	$(GO) run ./cmd/colca-bench check --thresholds bench/thresholds.json

clean: ## remove build output
	rm -rf bin build
