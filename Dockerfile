# One stage, one binary. No Python anywhere.
FROM rust:1-slim AS build
WORKDIR /build
COPY core/ .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY flows/ flows/
COPY services/ services/
COPY packages/ packages/
COPY docs/ docs/
COPY --from=build /build/target/release/vejas-runtime /usr/local/bin/vejas-runtime
ENV VEJAS_ROOT=/app
EXPOSE 8686 8787
CMD ["vejas-runtime"]
