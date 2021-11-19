upgrade:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
release:
	cargo build --release

build:
	cargo build

fix:
	cargo fix

install_linux:
	rustup target add x86_64-unknown-linux-gnu

linux:
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc cargo build --release --target=x86_64-unknown-linux-gnu

prepare_cross_compile:
	rustup target add x86_64-unknown-linux-gnu
	brew tap SergioBenitez/osxct
	brew install x86_64-unknown-linux-gnu

test:
	cargo test

coverage:
	cargo kcov

check:
	cargo check

docker-run:
	docker run  --env HOST_COUNT=1 --publish 21211:21211 --publish 8080:8080 voltdb/voltdb-community:9.2.1

gen:
	cargo run --bin force-build --features build_deps

clean_container:
	docker stop $$(docker ps -a -q --filter ancestor="voltdb/voltdb-community:9.2.1" --format="{{.ID}}")
	docker rm $$(docker ps -a -q --filter ancestor="voltdb/voltdb-community:9.2.1" --format="{{.ID}}")
