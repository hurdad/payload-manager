.PHONY: generate build

generate:
	buf generate --include-imports
	python3 scripts/merge_swagger.py

build:
	$(MAKE) -C gateway build
