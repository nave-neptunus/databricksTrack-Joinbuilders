# Shopping Sessions: Rust pipeline (rust/). The Python/DuckDB fallback is Dockerfile.python.
# Match the Rust and LLVM versions used to generate the profile.
FROM rust:1.100.0-slim-bookworm@sha256:7c7d4eb4d05b7781187a327809466cfddf29753f6defdc3d19a0e291951b7a37 AS build
ARG TARGETARCH
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
COPY rust/shopping.profdata ./shopping.profdata
# Portable baseline the Xeon scorers all support (AVX2, BMI2, FMA). Never target-cpu=native:
# the image may be built on a different CPU than the one it runs on.
RUN if [ "$TARGETARCH" = "amd64" ]; then \
      export RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-use=/src/shopping.profdata"; \
    fi; \
    cargo build --locked --release && strip target/release/rproto

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/rproto /app/rproto
USER 1000:1000
CMD ["/app/rproto"]
