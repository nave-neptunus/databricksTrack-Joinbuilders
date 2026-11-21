# Stage 1: the fast path (Team 67's Express Lane design, MIT; see README).
FROM rust:1.100.0-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
COPY pgo/fast.profdata pgo/fast.profdata
ENV RUSTFLAGS="-C target-cpu=cascadelake -C relocation-model=static -C llvm-args=-x86-branches-within-32B-boundaries -C llvm-args=-x86-pad-max-prefix-size=5 -Cprofile-use=/src/pgo/fast.profdata -Cllvm-args=-pgo-warn-mismatch=false"
RUN cargo build --release --target x86_64-unknown-linux-musl \
 && cp target/x86_64-unknown-linux-musl/release/pipeline /pipeline

# Stage 2: the general pipeline (ours) that the fast path hands off to when the
# data breaks its assumptions (long or opaque IDs, unusual categories, ...).
FROM rust:1.100.0-slim-bookworm AS fallback
WORKDIR /src
COPY fallback/Cargo.toml fallback/Cargo.lock ./
COPY fallback/src ./src
COPY fallback/pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -Cprofile-use=/src/pgo/merged.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/pipeline /fallback \
 && chmod 0755 /fallback

FROM scratch
COPY --from=build /pipeline /pipeline
COPY --from=fallback /fallback /fallback
USER 1000:1000
CMD ["/pipeline"]
