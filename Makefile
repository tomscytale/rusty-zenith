.PHONY: clippy
	cargo clippy

.PHONY: build
build:
	cargo build

.PHONY: fmt
fmt:
	rustfmt --edition 2021 src/*

.PHONY: fmt-build
fmt-build: fmt build
