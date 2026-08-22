# One stage, one binary. No Python anywhere.
# Build and run stages MUST share the same Debian release (glibc), hence the
# explicit -trixie pins on both.
FROM rust:1-slim-trixie AS build
WORKDIR /build
COPY core/ .
RUN cargo build --release

FROM debian:trixie-slim
# ca-certificates only: the runtime's HTTP (webhooks, oauth-poll, http-poll) goes
# through the in-binary ureq/rustls client — no `curl` shell-out anymore, so it is
# not installed. The connector crates under connectors/ that still use curl are
# not built into this image (it ships only vejas-runtime).
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
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
