# One stage, one binary. No Python anywhere.
# Build and run stages MUST share the same Debian release (glibc), hence the
# explicit -trixie pins on both.
FROM rust:1-slim-trixie AS build
WORKDIR /build
COPY core/ .
RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY flows/ flows/
COPY services/ services/
COPY packages/ packages/
COPY connectors/ connectors/
COPY docs/ docs/
COPY --from=build /build/target/release/vejas-runtime /usr/local/bin/vejas-runtime
ENV VEJAS_ROOT=/app
EXPOSE 8686 8787
CMD ["vejas-runtime"]
