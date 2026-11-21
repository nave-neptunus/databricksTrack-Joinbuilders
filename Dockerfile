# Shopping Sessions: Rust pipeline (rust/). The Python/DuckDB fallback is Dockerfile.python.
FROM rust:1-slim-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
# Keep the entrypoint portable across Xeons. The v3 binary selects its v4 sibling
# only after checking the CPU and OS. Other architectures build one binary.
RUN mkdir /out \
    && if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi \
    && cargo build --locked --release \
    && strip -o /out/rproto target/release/rproto \
    && if [ "$TARGETARCH" = "amd64" ]; then \
        RUSTFLAGS="-C target-cpu=x86-64-v4" cargo build --locked --release \
        && strip -o /out/rproto-v4 target/release/rproto; \
    fi

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /out/ /app/
# Back malloc with transparent huge pages (glibc >= 2.35; bookworm has 2.36). Cuts ~450K 4K page
# faults to a few hundred; measured -20% wall on Linux at NCPU=8 with identical output.
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000:1000
CMD ["/app/rproto"]
