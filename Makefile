# Build, test and run Colca. `make help` lists the targets.
GO       ?= go
UV       ?= uv
IMAGE    ?= colca:dev
BUNDLE   := build/bundle/contracts-bundle.json
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
BINARIES := colcad colca-keygen colca-machine colca-grantsync colca-historian colca-service \
            colca-healthcheck colca-volume-init colca-bench

# Linter versions, the same as in .github/workflows/ci.yml.
GOLANGCI_LINT ?= $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
GOVULNCHECK   ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@v1.8.0
ACTIONLINT    ?= $(GO) run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
HADOLINT      ?= docker run --rm -i hadolint/hadolint:v2.15.1-alpine hadolint
YAMLLINT      ?= $(UV) tool run yamllint@1.38.0

.PHONY: help test contracts-test check lint lint-go lint-python lint-docker lint-shell lint-actions \
        lint-yaml bundle build docker smoke demo ci wheels bench bench-scenarios bench-check clean

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

lint: lint-go lint-python lint-docker lint-shell lint-actions lint-yaml ## every linter CI runs

lint-go: ## golangci-lint and govulncheck
	$(GOLANGCI_LINT) run ./...
	$(GOVULNCHECK) ./...

lint-python: ## ruff, bandit and mypy on the data contracts
	$(UV) run --project contracts --group lint ruff check contracts
	$(UV) run --project contracts --group lint ruff format --check contracts
	$(UV) run --project contracts --group lint bandit -q -c contracts/pyproject.toml -r contracts/src
	$(UV) run --project contracts --group lint mypy --config-file contracts/pyproject.toml contracts/src

lint-docker: ## hadolint
	$(HADOLINT) - < deploy/Dockerfile

lint-shell: ## shellcheck
	git ls-files -z '*.sh' | xargs -0 shellcheck

lint-actions: ## actionlint
	$(ACTIONLINT)

lint-yaml: ## yamllint
	git ls-files -z '*.yml' '*.yaml' | xargs -0 $(YAMLLINT) --strict

bundle: ## the contracts schema bundle for this commit
	@mkdir -p $(dir $(BUNDLE))
	$(UV) run --project contracts python contracts/scripts/generate_bundle.py $(BUNDLE) $(COMMIT)

build: ## all binaries into bin/
	@mkdir -p bin
	@for b in $(BINARIES); do echo "go build $$b"; \
	  CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/$$b ./cmd/$$b || exit 1; \
	done

docker: bundle ## the colca image, with this commit's bundle baked in
	docker build -f deploy/Dockerfile -t $(IMAGE) --build-arg VERSION=$(VERSION) \
	  --build-context contracts-bundle=$(dir $(BUNDLE)) .

smoke: docker ## start the four-node demo tree and assert on it
	COLCA_IMAGE=$(IMAGE) bash demo/smoke.sh

demo: docker ## the same, with narration
	COLCA_IMAGE=$(IMAGE) bash demo/demo.sh

ci: check lint test contracts-test smoke ## everything CI runs

wheels: ## colcad platform wheels for chaski[node], VERSION=x.y.z
	@test "$(origin VERSION)" = "command line" || { echo "usage: make wheels VERSION=x.y.z"; exit 2; }
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
