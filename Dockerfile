# Stage 1: build the runtime
FROM rust:1-slim AS build
WORKDIR /build
COPY core/ .
RUN cargo build --release

# Stage 2: runtime + Python for the bundled SDK/connectors/flows
FROM python:3.12-slim
RUN pip install --no-cache-dir "nats-py>=2.6"
WORKDIR /app
COPY sdk/ sdk/
COPY connectors/ connectors/
COPY flows/ flows/
COPY docs/ docs/
COPY --from=build /build/target/release/vejas-runtime /usr/local/bin/vejas-runtime
ENV VEJAS_ROOT=/app
EXPOSE 8686 8787
CMD ["vejas-runtime"]
