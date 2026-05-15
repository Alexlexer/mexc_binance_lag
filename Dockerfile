FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY dashboard.html ./
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/mexc_lag /usr/local/bin/mexc_lag
COPY config.json ./

RUN useradd --system --create-home --uid 10001 app \
    && chown -R app:app /app

USER app
EXPOSE 8787
CMD ["/usr/local/bin/mexc_lag"]
