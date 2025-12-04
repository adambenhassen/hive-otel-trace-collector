.PHONY: build build-release build-linux test test-unit test-integration test-e2e clean

build:
	cargo build

build-release:
	cargo build --release

build-linux:
	cargo zigbuild --release --target x86_64-unknown-linux-gnu

test:
	cargo test

test-integration:
	cargo test --test integration_test

test-e2e:
	cargo build && cargo test --test e2e_test

clean:
	cargo clean
