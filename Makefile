# go options
GO         ?= go
PKG        := go mod vendor
LDFLAGS    := -w -s
GOFLAGS    :=
TAGS       := 
PKGDIR     := git.thm.de/gsas42/wikidata-importer/cmd/wikidata-importer

# Required for globs to work correctly
SHELL=/bin/bash

.PHONY: all
all: build

.PHONY: dep
dep:
	@echo " ===> Installing dependencies via '$$(awk '{ print $$1 }' <<< "$(PKG)" )' <=== "
	@$(PKG)

.PHONY: build
build:
	@echo " ===> building releases in ./bin/... <=== "
	CGO_ENABLED=1 $(GO) build -o ./bin/wikidata-importer -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' $(PKGDIR)...

.PHONY: clean
clean:
	@echo " ===> cleaning ./bin/... <=== "
	rm -f ./bin/wikidata-importer

.PHONY: gofmt
gofmt:
	@echo " ===> Running go fmt <==="
	gofmt -w ./
