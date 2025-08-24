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

.PHONY: build-sysroot
build-sysroot:
	docker build -f Dockerfile.sysroot -t tailpipe-sysroot:bookworm .
	docker create --name temp-sysroot tailpipe-sysroot:bookworm
	docker cp temp-sysroot:/sysroot ./sysroot
	docker rm temp-sysroot

.PHONY: release-dry-run
release-dry-run: build-sysroot
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-e PKG_CONFIG_SYSROOT_DIR=/sysroot/linux/amd64-bookworm \
		-e PKG_CONFIG_PATH=/sysroot/linux/amd64-bookworm/usr/local/lib/pkgconfig \
		-e CC=/sysroot/linux/amd64-bookworm/usr/bin/gcc \
		-e CXX=/sysroot/linux/amd64-bookworm/usr/bin/g++ \
		-e CGO_LDFLAGS="-L/sysroot/linux/amd64-bookworm/usr/lib/x86_64-linux-gnu -L/sysroot/linux/amd64-bookworm/lib/x86_64-linux-gnu -lstdc++ -static-libstdc++" \
		-e CGO_CXXFLAGS="-I/sysroot/linux/amd64-bookworm/usr/include/c++/12 -I/sysroot/linux/amd64-bookworm/usr/include/x86_64-linux-gnu/c++/12" \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-v `pwd`/sysroot:/sysroot \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip=validate --skip=publish --snapshot

.PHONY: release-acceptance
release-acceptance: build-sysroot
	@docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-e PKG_CONFIG_SYSROOT_DIR=/sysroot/linux/amd64-bookworm \
		-e PKG_CONFIG_PATH=/sysroot/linux/amd64-bookworm/usr/local/lib/pkgconfig \
		-e CC=/sysroot/linux/amd64-bookworm/usr/bin/gcc \
		-e CXX=/sysroot/linux/amd64-bookworm/usr/bin/g++ \
		-e CGO_LDFLAGS="-L/sysroot/linux/amd64-bookworm/usr/lib/x86_64-linux-gnu -L/sysroot/linux/amd64-bookworm/lib/x86_64-linux-gnu -lstdc++ -static-libstdc++" \
		-e CGO_CXXFLAGS="-I/sysroot/linux/amd64-bookworm/usr/include/c++/12 -I/sysroot/linux/amd64-bookworm/usr/include/x86_64-linux-gnu/c++/12" \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-v `pwd`/sysroot:/sysroot \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip=validate --skip=publish --snapshot --config=.acceptance.goreleaser.yml

.PHONY: release
release: build-sysroot
	@if [ ! -f ".release-env" ]; then \
		echo ".release-env is required for release";\
		exit 1;\
	fi
	docker run \
		--rm \
		-e CGO_ENABLED=1 \
		-e PKG_CONFIG_SYSROOT_DIR=/sysroot/linux/amd64-bookworm \
		-e PKG_CONFIG_PATH=/sysroot/linux/amd64-bookworm/usr/local/lib/pkgconfig \
		-e CC=/sysroot/linux/amd64-bookworm/usr/bin/gcc \
		-e CXX=/sysroot/linux/amd64-bookworm/usr/bin/g++ \
		-e CGO_LDFLAGS="-L/sysroot/linux/amd64-bookworm/usr/lib/x86_64-linux-gnu -L/sysroot/linux/amd64-bookworm/lib/x86_64-linux-gnu -lstdc++ -static-libstdc++" \
		-e CGO_CXXFLAGS="-I/sysroot/linux/amd64-bookworm/usr/include/c++/12 -I/sysroot/linux/amd64-bookworm/usr/include/x86_64-linux-gnu/c++/12" \
		--env-file .release-env \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/tailpipe \
		-v `pwd`/../pipe-fittings:/go/src/pipe-fittings \
		-v `pwd`/../tailpipe-plugin-sdk:/go/src/tailpipe-plugin-sdk \
		-v `pwd`/../tailpipe-plugin-core:/go/src/tailpipe-plugin-core \
		-v `pwd`/sysroot:/sysroot \
		-w /go/src/tailpipe \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release --clean --skip=validate
