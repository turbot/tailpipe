OUTPUT_DIR?=/usr/local/bin
PACKAGE_NAME          := github.com/turbot/tailpipe
GOLANG_CROSS_VERSION  ?= v1.22.4

.PHONY: build
build:
	$(eval MAJOR := $(shell cat internal/version/version.json | jq '.major'))
	$(eval MINOR := $(shell cat internal/version/version.json | jq '.minor'))
	$(eval PATCH := $(shell cat internal/version/version.json | jq '.patch'))
	$(eval TIMESTAMP := $(shell date +%Y%m%d%H%M%S))

	go build -o $(OUTPUT_DIR) -ldflags "-X main.version=$(MAJOR).$(MINOR).$(PATCH)-dev.$(TIMESTAMP)" .


.PHONY: release-dry-run
release-dry-run:
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../pipe-fittings:/go/src/tailpipe-plugin-sdk \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip=validate --skip=publish --snapshot


.PHONY: release
release:
	@if [ ! -f ".release-env" ]; then \
		echo ".release-env is required for release";\
		exit 1;\
	fi
	docker run \
		--rm \
		-e CGO_ENABLED=1 \
		--env-file .release-env \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../pipe-fittings:/go/src/tailpipe-plugin-sdk \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release --clean --skip=validate
