FROM ubuntu:latest AS builder

ARG BUILD_PROFILE=release

RUN apt update -y
RUN apt install curl git -y
RUN curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install linux \
  --extra-conf "sandbox = false" \
  --extra-conf "experimental-features = nix-command flakes" \
  --init none \
  --no-confirm
ENV PATH="${PATH}:/nix/var/nix/profiles/default/bin"

WORKDIR /app

COPY flake.nix flake.lock ./
RUN nix develop --command echo "Nix dev env ready"

COPY . .

RUN nix run .#prepSolArtifacts

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    sqlx database create && \
    sqlx migrate run \
'

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    cargo test -q \
'

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    if [ "$BUILD_PROFILE" = "release" ]; then \
        cargo build --release --bin server && \
        cargo build --release --bin reporter; \
    else \
        cargo build --bin server && \
        cargo build --bin reporter; \
    fi'

# Fix binary interpreter path to use standard Linux paths
RUN apt-get update && apt-get install -y --no-install-recommends patchelf && \
    patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 /app/target/${BUILD_PROFILE}/server && \
    patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 /app/target/${BUILD_PROFILE}/reporter && \
    apt-get remove -y patchelf && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

FROM debian:12-slim

ARG BUILD_PROFILE=release

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only the compiled binaries from builder stage (now with fixed interpreter paths)
COPY --from=builder /app/target/${BUILD_PROFILE}/server ./
COPY --from=builder /app/target/${BUILD_PROFILE}/reporter ./
COPY --from=builder /app/migrations ./migrations
