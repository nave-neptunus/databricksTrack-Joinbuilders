# Shopping Sessions: Rust pipeline (rust/). The Python/DuckDB fallback is Dockerfile.python.
FROM rust:1-slim-bookworm AS build
ARG TARGETARCH
ARG NOVA_SHARDS=1024
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
# Portable baseline the Xeon scorers all support (AVX2, BMI2, FMA). Never target-cpu=native:
# the image may be built on a different CPU than the one it runs on.
RUN case "$NOVA_SHARDS" in \
      256) features= ;; \
      512|1024|2048) features="--features nova-shards-$NOVA_SHARDS" ;; \
      *) echo "NOVA_SHARDS must be 256, 512, 1024, or 2048" >&2; exit 2 ;; \
    esac; \
    if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    cargo build --release $features && strip target/release/rproto

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/rproto /app/rproto
# Back malloc with transparent huge pages (glibc >= 2.35; bookworm has 2.36). Cuts ~450K 4K page
# faults to a few hundred; measured -20% wall on Linux at NCPU=8 with identical output.
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000:1000
CMD ["/app/rproto"]
