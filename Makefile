COREDNS_VERSION ?= 1.14.2
COREDNS_SOURCE_URL ?= https://github.com/coredns/coredns/archive/refs/tags/v$(COREDNS_VERSION).tar.gz
VERSION_SUFFIX ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

COREDNS_DIR = coredns-$(COREDNS_VERSION)
PATCH_FILE = patches/$(COREDNS_VERSION)/coredns.patch
TARBALL = coredns.tar.gz
BINARY = coredns

.PHONY: all build test download extract patch version clean

all: build

build: $(COREDNS_DIR)/$(BINARY)

test:
	go test ./...

$(COREDNS_DIR)/$(BINARY): $(COREDNS_DIR)/.patched
	cd $(COREDNS_DIR) && go mod tidy && make

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

clean:
	rm -rf $(COREDNS_DIR) $(TARBALL)
