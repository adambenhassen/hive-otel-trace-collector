.PHONY: build build-release build-linux test test-unit test-integration clean

build:
	cargo build

build-release:
	cargo build --release

build-linux:
	cargo zigbuild --release --target x86_64-unknown-linux-gnu

test:
	cargo test

test-unit:
	cargo test --lib

test-integration:
	cargo test --test integration_test

clean:
	cargo clean
