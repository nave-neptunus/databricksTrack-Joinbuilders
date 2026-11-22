# Shopping Sessions: Rust pipeline (rust/). The Python/DuckDB fallback is Dockerfile.python.
FROM rust:1-slim-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
# Static glibc binary for AVX-512 scorers. The scorer hosts are amd64; keep the branch-alignment
# flags from the static leader images. Never target-cpu=native: the image may be built on a
# different CPU than the one it runs on. Target-specific flags keep host proc-macros dynamic.
RUN if [ "$TARGETARCH" = "amd64" ]; then \
      export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -C relocation-model=static -C llvm-args=-x86-branches-within-32B-boundaries -C llvm-args=-x86-pad-max-prefix-size=5"; \
      cargo build --release --locked --target x86_64-unknown-linux-gnu; \
      BIN=target/x86_64-unknown-linux-gnu/release/rproto; \
    else \
      cargo build --release --locked; \
      BIN=target/release/rproto; \
    fi; \
    strip "$BIN"

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/rproto /app/rproto
# Static glibc reads tunables at startup, so huge-page malloc still applies without libc mounts.
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
ENV BLUE2_IO_THREADS=32
USER 1000:1000
CMD ["/app/rproto"]
