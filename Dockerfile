# One image for every role: coordinator, worker, or both (ADR 0123).
FROM rust:1.98-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p plimsoll-server \
    && cp target/release/plimsoll-server /usr/local/bin/

FROM debian:bookworm-slim
# Roots for https catalogs, object stores and OTLP collectors.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home plimsoll
COPY --from=build /usr/local/bin/plimsoll-server /usr/local/bin/
USER plimsoll
ENV PLIMSOLL_BIND=0.0.0.0:8080
EXPOSE 8080 32010
ENTRYPOINT ["plimsoll-server"]
