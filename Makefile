OUTPUT_DIR?=/usr/local/bin
PACKAGE_NAME          := github.com/turbot/tailpipe
GOLANG_CROSS_VERSION  ?= v1.22.4

.PHONY: build
build:
	$(eval TIMESTAMP := $(shell date +%Y%m%d%H%M%S))
	$(eval GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD | sed 's/[\/]/-/g'))

	go build -o $(OUTPUT_DIR) -ldflags "-X main.version=0.0.0-dev-$(GIT_BRANCH).$(TIMESTAMP)" .

.PHONY: release-dry-run
release-dry-run:
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
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
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release --clean --skip=validate
