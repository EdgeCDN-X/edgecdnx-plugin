COREDNS_VERSION ?= 1.14.2
COREDNS_SOURCE_URL ?= https://github.com/coredns/coredns/archive/refs/tags/v$(COREDNS_VERSION).tar.gz
VERSION_SUFFIX ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
GO ?= go
GOFMT ?= gofmt
GOFLAGS ?=
DOCKER ?= docker
KIND ?= kind
KUBECTL ?= kubectl
HELM ?= helm
PWSH ?= pwsh
PYTHON ?= python
KIND_CLUSTER ?= edgeroute
KUBE_CONTEXT ?= kind-$(KIND_CLUSTER)
KIND_NODE_IMAGE ?= kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5
COREDNS_IMAGE ?= edgeroute-coredns:dev
CONTROLLER_IMAGE ?= edgeroute-quality-controller:dev
K6_IMAGE ?= edgeroute-k6:1.8.0
MONITORING_CHART_VERSION ?= 88.5.4
EDGECDNX_CONTROLLER_COMMIT ?= aafe2fee61194890c365b16a5a9107c3e09d1cc9
FUZZ_TIME ?= 10s
EXPERIMENT_PROFILE ?= smoke
EXPERIMENT_REPETITIONS ?= 3

COREDNS_DIR = coredns-$(COREDNS_VERSION)
PATCH_FILE = patches/$(COREDNS_VERSION)/coredns.patch
TARBALL = coredns.tar.gz
BINARY = coredns

.PHONY: all fmt fmt-check lint vet test test-race fuzz benchmark build image load-images \
	download extract patch version kind-up kind-down install-crds monitoring deploy \
	stream-start smoke-test experiment-baseline experiment-adaptive collect-results e2e quick-start clean

all: build

fmt:
	$(GOFMT) -w $$(find . -name '*.go' -not -path './coredns-*/*')

fmt-check:
	@FILES="$$( $(GOFMT) -l $$(find . -name '*.go' -not -path './coredns-*/*') )"; \
	if [ -n "$$FILES" ]; then echo "gofmt required:"; echo "$$FILES"; exit 1; fi

vet:
	$(GO) vet $(GOFLAGS) ./...

lint: fmt-check vet

test:
	$(GO) test $(GOFLAGS) ./...

test-race:
	$(GO) test $(GOFLAGS) -race ./...

fuzz:
	$(GO) test $(GOFLAGS) ./internal/routing -run='^$$' -fuzz='^FuzzBuildRoutingKeyStable$$' -fuzztime=$(FUZZ_TIME)
	$(GO) test $(GOFLAGS) ./internal/routing -run='^$$' -fuzz='^FuzzWeightedRendezvousNeverSelectsInvalidCandidate$$' -fuzztime=$(FUZZ_TIME)

benchmark:
	$(GO) test $(GOFLAGS) ./internal/routing -run='^$$' -bench=. -benchmem

build: $(COREDNS_DIR)/$(BINARY)

image: build
	cp $(COREDNS_DIR)/$(BINARY) ./coredns
	$(DOCKER) build --platform linux/amd64 --tag $(COREDNS_IMAGE) .
	$(DOCKER) build --platform linux/amd64 --file Dockerfile.controller --tag $(CONTROLLER_IMAGE) .

load-images: image
	$(KIND) load docker-image $(COREDNS_IMAGE) $(CONTROLLER_IMAGE) --name $(KIND_CLUSTER)

$(COREDNS_DIR)/$(BINARY): $(COREDNS_DIR)/.patched version
	cd $(COREDNS_DIR) && CGO_ENABLED=0 go mod tidy && CGO_ENABLED=0 make

version: $(COREDNS_DIR)/.patched
	@NEW_VERSION="$(COREDNS_VERSION)-edgecdnx-$(VERSION_SUFFIX)"; \
	echo "Setting CoreDNS version to: $$NEW_VERSION"; \
	sed -i.bak 's/CoreVersion = "[^"]*"/CoreVersion = "'"$$NEW_VERSION"'"/' $(COREDNS_DIR)/coremain/version.go && \
	rm -f $(COREDNS_DIR)/coremain/version.go.bak

$(COREDNS_DIR)/.patched: $(COREDNS_DIR)/.extracted
	@if [ ! -f "$(PATCH_FILE)" ]; then \
		echo "Error: patch file not found at $(PATCH_FILE)"; exit 1; \
	fi
	cd $(COREDNS_DIR) && patch -p1 < ../$(PATCH_FILE)
	@touch $@

$(COREDNS_DIR)/.extracted: $(TARBALL)
	tar -xzf $(TARBALL)
	@touch $@
	@NEW_VERSION="$(COREDNS_VERSION)-edgecdnx-$(VERSION_SUFFIX)"; \
	echo "Setting CoreDNS version to: $$NEW_VERSION"; \
	sed -i.bak 's/CoreVersion = "[^"]*"/CoreVersion = "'"$$NEW_VERSION"'"/' $(COREDNS_DIR)/coremain/version.go && \
	rm -f $(COREDNS_DIR)/coremain/version.go.bak; \
	echo "Updated version:"; \
	grep "CoreVersion" $(COREDNS_DIR)/coremain/version.go

$(TARBALL):
	@echo "Downloading CoreDNS $(COREDNS_VERSION) from $(COREDNS_SOURCE_URL)..."
	curl -L -o $(TARBALL) "$(COREDNS_SOURCE_URL)"

download: $(TARBALL)

extract: $(COREDNS_DIR)/.extracted

patch: $(COREDNS_DIR)/.patched

kind-up:
	$(KIND) create cluster --name $(KIND_CLUSTER) --image $(KIND_NODE_IMAGE) --config deploy/kind/cluster.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) wait --for=condition=Ready nodes --all --timeout=180s

kind-down:
	$(KIND) delete cluster --name $(KIND_CLUSTER)

install-crds:
	$(KUBECTL) --context $(KUBE_CONTEXT) apply -f https://raw.githubusercontent.com/EdgeCDN-X/edgecdnx-controller/$(EDGECDNX_CONTROLLER_COMMIT)/config/crd/bases/infrastructure.edgecdnx.com_locations.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) apply -f https://raw.githubusercontent.com/EdgeCDN-X/edgecdnx-controller/$(EDGECDNX_CONTROLLER_COMMIT)/config/crd/bases/infrastructure.edgecdnx.com_prefixlists.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) apply -f https://raw.githubusercontent.com/EdgeCDN-X/edgecdnx-controller/$(EDGECDNX_CONTROLLER_COMMIT)/config/crd/bases/infrastructure.edgecdnx.com_projects.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) apply -f https://raw.githubusercontent.com/EdgeCDN-X/edgecdnx-controller/$(EDGECDNX_CONTROLLER_COMMIT)/config/crd/bases/infrastructure.edgecdnx.com_services.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) apply -f https://raw.githubusercontent.com/EdgeCDN-X/edgecdnx-controller/$(EDGECDNX_CONTROLLER_COMMIT)/config/crd/bases/infrastructure.edgecdnx.com_zones.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) apply -f config/crd/adaptive.edgecdnx.io_nodequalities.yaml

monitoring:
	$(HELM) --kube-context $(KUBE_CONTEXT) upgrade --install monitoring oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack --version $(MONITORING_CHART_VERSION) --namespace monitoring --create-namespace --values deploy/monitoring/values.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) -n monitoring rollout status deployment/monitoring-kube-prometheus-operator --timeout=300s

deploy:
	$(KUBECTL) --context $(KUBE_CONTEXT) create namespace edge-system --dry-run=client -o yaml | $(KUBECTL) --context $(KUBE_CONTEXT) apply -f -
	$(KUBECTL) --context $(KUBE_CONTEXT) create namespace edge-data --dry-run=client -o yaml | $(KUBECTL) --context $(KUBE_CONTEXT) apply -f -
	$(KUBECTL) --context $(KUBE_CONTEXT) apply --server-side --force-conflicts -k deploy
	$(KUBECTL) --context $(KUBE_CONTEXT) apply -f config/samples/nodequalities.yaml
	$(KUBECTL) --context $(KUBE_CONTEXT) -n edge-system rollout status deployment/edgeroute-coredns --timeout=300s
	$(KUBECTL) --context $(KUBE_CONTEXT) -n edge-system rollout status deployment/quality-controller --timeout=300s

stream-start:
	$(KUBECTL) --context $(KUBE_CONTEXT) -n edge-data rollout status deployment/mediamtx --timeout=180s
	$(KUBECTL) --context $(KUBE_CONTEXT) -n edge-data rollout status deployment/ffmpeg-publisher --timeout=180s

smoke-test:
	$(PWSH) -NoProfile -File scripts/verify-hls.ps1 -Context $(KUBE_CONTEXT)

experiment-baseline:
	$(PWSH) -NoProfile -File experiments/run-day6.ps1 -Profile $(EXPERIMENT_PROFILE) -Variants baseline -Repetitions $(EXPERIMENT_REPETITIONS) -Context $(KUBE_CONTEXT) -SourceImage $(COREDNS_IMAGE)

experiment-adaptive:
	$(PWSH) -NoProfile -File experiments/run-day6.ps1 -Profile $(EXPERIMENT_PROFILE) -Variants adaptive -Repetitions $(EXPERIMENT_REPETITIONS) -Context $(KUBE_CONTEXT) -SourceImage $(COREDNS_IMAGE)

collect-results:
	$(PYTHON) experiments/process_results.py

e2e:
	$(PWSH) -NoProfile -File scripts/e2e-smoke.ps1 -Context $(KUBE_CONTEXT) -SourceImage $(COREDNS_IMAGE)

quick-start: kind-up install-crds monitoring load-images deploy stream-start smoke-test e2e

clean:
	rm -rf $(COREDNS_DIR) $(TARBALL)
