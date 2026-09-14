.PHONY: generate build

# scripts/codegen.sh is the one generation path: it pins the Go toolchain and
# every plugin version, runs buf, merges the swagger, and regenerates the
# Python stubs. This target used to run its own `buf generate --include-imports`
# with no pins, which produced different output from CI and from
# gateway/Makefile — three paths, and the merged API doc fell behind all of them.
generate:
	scripts/codegen.sh all

build:
	$(MAKE) -C gateway build
