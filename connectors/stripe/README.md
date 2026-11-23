# Vejas Stripe Connector

Native Rust connector for Stripe API integration, following the exec-process architecture (ADR-0011).

## Architecture

This connector is a **standalone Rust binary** that runs as an isolated process, supervised by the Vejas runtime. It follows the same pattern as the SAP and Salesforce connectors.

### Two Modes

1. **Events Mode** (`exec-stream-source`): Polls Stripe Events API and streams events to the bus
2. **Payment Mode** (`exec-sink`): Consumes payment requests from the bus and creates Stripe Payment Intents

## Building

```bash
cd connectors/stripe
cargo build --release
```

Binary output: `target/release/vejas-stripe`

## Configuration

### Events Polling (Source)

Manifest: `stripe_events_poll.vjs`

```javascript
driver "exec-stream-source"
SUBJECT = "vx.stripe.events"
COMMAND = "./connectors/stripe/target/release/vejas-stripe"
ARGS = ["events"]
ENV = {
    "STRIPE_API_KEY": secret("stripe/api_key"),
    "STRIPE_INTERVAL_SECS": "60"
}
```

**Environment Variables:**
- `STRIPE_API_KEY` (required): Your Stripe API key (sk_test_... or sk_live_...)
- `STRIPE_INTERVAL_SECS` (optional, default: 60): Polling interval in seconds

### Payment Intent Creation (Sink)

Manifest: `stripe_create_payment.vjs`

```javascript
driver "exec-sink"
SUBJECT = "vx.stripe.create_payment"
COMMAND = "./connectors/stripe/target/release/vejas-stripe"
ARGS = ["payment"]
ENV = {
    "STRIPE_API_KEY": secret("stripe/api_key")
}
```

**Input Format (stdin):**
```json
{
  "amount": 5000,
  "currency": "eur",
  "customer": "cus_...",
  "description": "Order #1234"
}
```

**Output Format (stdout):**
```json
{
  "ok": true,
  "id": "pi_...",
  "amount": 5000,
  "currency": "eur",
  "status": "requires_payment_method",
  "client_secret": "pi_...secret..."
}
```

## Security

- **No credentials in argv**: API keys are passed via environment variables (ADR-0008)
- **HTTP via curl**: All HTTP requests use curl with 0600 config files
- **Process isolation**: Runs as separate process with its own memory space

## Testing

See `docs/examples/connectors/stripe/` for admission tests and mock server.

## Comparison with Built-in Drivers

**Previous approach** (docs/examples):
- Used built-in `http-poll` and `http-out` drivers
- Declarative manifest only
- No custom code

**New approach** (connectors/stripe):
- Standalone Rust binary
- Full control over HTTP, pagination, error handling
- Consistent with SAP/Salesforce architecture
- Distributed as compiled binary with the runtime

## License

Apache-2.0
