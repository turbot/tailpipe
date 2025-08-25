OUTPUT_DIR?=/usr/local/bin
PACKAGE_NAME          := github.com/turbot/tailpipe
GOLANG_CROSS_VERSION  ?= v1.25.0

# sed 's/[\/_]/-/g': Replaces both slashes (/) and underscores (_) with hyphens (-).
# sed 's/[^a-zA-Z0-9.-]//g': Removes any character that isn't alphanumeric, a dot (.), or a hyphen (-).
# This is to ensure that the branch name is a valid semver pre-release identifier.
.PHONY: build
build:
	$(eval TIMESTAMP := $(shell date +%Y%m%d%H%M%S))
	$(eval GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD | sed 's/[\/_]/-/g' | sed 's/[^a-zA-Z0-9.-]//g'))

	go build -o $(OUTPUT_DIR) -ldflags "-X main.version=0.0.0-dev-$(GIT_BRANCH).$(TIMESTAMP)" .

.PHONY: build-goreleaser-image
build-goreleaser-image:
	docker build -f Dockerfile.goreleaser-cross -t tailpipe-goreleaser-cross:gcc13 .

.PHONY: release-dry-run
release-dry-run: build-goreleaser-image
	@echo "Building for Linux platforms using custom image with GCC 13+..."
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-w /go/src/tailpipe \
		tailpipe-goreleaser-cross:gcc13 \
		--clean --skip=validate --skip=publish --snapshot

.PHONY: release-acceptance
release-acceptance: build-goreleaser-image
	@echo "Building for acceptance testing using custom image with GCC 13+..."
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-w /go/src/tailpipe \
		tailpipe-goreleaser-cross:gcc13 \
		--clean --skip=validate --skip=publish --snapshot --config=.acceptance.goreleaser.yml

.PHONY: release
release: build-goreleaser-image
	@if [ ! -f ".release-env" ]; then \
		echo ".release-env is required for release";\
		exit 1;\
	fi
	@echo "Building for all platforms (Linux + Darwin) for release..."
	@echo "Linux builds: Using custom image with GCC 13+"
	@echo "Darwin builds: Using standard goreleaser-cross"
	@echo ""
	@echo "Building Linux targets..."
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		--env-file .release-env \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-w /go/src/tailpipe \
		tailpipe-goreleaser-cross:gcc13 \
		release --clean --skip=validate
	@echo ""
	@echo "Building Darwin targets..."
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		--env-file .release-env \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release --clean --skip=validate --config=.darwin.goreleaser.yml
	@echo ""
	@echo "✅ Release builds completed successfully!"
	@echo "📦 Linux builds: AMD64, ARM64"
	@echo "🍎 Darwin builds: AMD64, ARM64"

# Darwin-only builds using standard goreleaser-cross
.PHONY: release-darwin
release-darwin:
	@echo "Building Darwin targets using standard goreleaser-cross..."
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip=validate --skip=publish --snapshot --config=.darwin.goreleaser.yml

# Build for all platforms (Linux + Darwin) - UNIFIED APPROACH
.PHONY: release-all-platforms
release-all-platforms: build-goreleaser-image
	@echo "Building for all platforms using unified approach..."
	@echo "Linux builds: Using custom image with GCC 13+"
	@echo "Darwin builds: Using standard goreleaser-cross"
	@echo ""
	@echo "Building Linux targets..."
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-w /go/src/tailpipe \
		tailpipe-goreleaser-cross:gcc13 \
		--clean --skip=validate --skip=publish --snapshot
	@echo ""
	@echo "Building Darwin targets..."
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip=validate --skip=publish --snapshot --config=.darwin.goreleaser.yml
	@echo ""
	@echo "✅ All platform builds completed successfully!"
	@echo "📦 Linux builds: AMD64, ARM64"
	@echo "🍎 Darwin builds: AMD64, ARM64"
