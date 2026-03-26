.PHONY: generate build

generate:
	buf generate

build:
	$(MAKE) -C gateway build
