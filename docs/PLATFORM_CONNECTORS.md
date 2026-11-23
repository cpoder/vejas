# Platform Connectors Architecture

## Philosophy

Vejas connectors are **platform-based**, not action-based. Each connector represents a complete integration with an external platform, supporting multiple operations through a single, well-configured driver instance.

## Connector Types

### 1. HTTP-based Platforms (via `http-poll`, `oauth-poll`, `http-out`)

These platforms expose REST APIs and are integrated using Vejas's built-in HTTP drivers:

- **`http-poll`**: Periodic polling with cursor pagination
- **`oauth-poll`**: OAuth2 client credentials + Bearer token + polling
- **`http-out`**: Outbound HTTP requests (webhooks, API calls)

**Examples:**
- **Stripe**: One `oauth-poll` connector accessing all Stripe APIs (payments, subscriptions, customers, invoices)
- **GitHub**: One `oauth-poll` connector for issues, PRs, repos, webhooks
- **Twilio**: One `http-out` connector for SMS, Voice, WhatsApp (Basic Auth)
- **Jira**: One `oauth-poll` connector for issues, projects, workflows

### 2. Cloud Provider Platforms (via `http-out` + AWS Signature v4)

Cloud providers require special authentication (AWS SigV4, GCP service accounts, Azure AD):

- **AWS**: S3, Lambda, SQS, SNS, DynamoDB operations
- **Google Cloud**: GCS, Pub/Sub, Cloud Functions
- **Azure**: Blob Storage, Service Bus, Functions

These use `http-out` with custom `HEADERS` for authentication.

### 3. Database Platforms (via `exec-*` drivers)

Databases require native protocol implementations, wrapped as exec connectors:

- **PostgreSQL**: `exec-rpc` for queries, transactions
- **MongoDB**: `exec-rpc` for CRUD operations
- **Redis**: `exec-rpc` for commands, pub/sub
- **Elasticsearch**: `exec-rpc` for indexing, search, bulk operations

### 4. Enterprise Platforms (via native Rust or `exec-*`)

Complex enterprise systems with native SDKs:

- **SAP**: Native Rust over `libsapnwrfc` (ADR-0014) - `exec-rpc` and `exec-stream-source`
- **Salesforce**: Native Rust connector (already exists in `connectors/salesforce/`)
- **IBM MQ**: Native Rust connector (already exists in `connectors/mq/`)
- **AMQP**: Native Rust connector (already exists in `connectors/amqp/`)

## Connector Structure

Each platform connector lives in `docs/examples/connectors/<platform>/`:

```
docs/examples/connectors/
├── stripe/
│   ├── stripe.vjs.example          # Main connector manifest
│   ├── fixture.json                # Sample output
│   ├── mock.mjs                    # Mock API server
│   └── overrides.json              # Optional test overrides
├── github/
│   ├── github.vjs.example
│   ├── fixture.json
│   ├── mock.mjs
│   └── overrides.json
└── aws/
    ├── aws.vjs.example
    ├── fixture.json
    ├── mock.mjs
    └── overrides.json
```

## Multi-Operation Pattern

A platform connector supports multiple operations through:

1. **Configuration-driven endpoints**: The manifest specifies base URL, auth, headers
2. **Flow-level operation selection**: Flows choose which API endpoint to call
3. **Shared authentication**: One set of credentials serves all operations

**Example: Stripe connector**

```javascript
// Connector manifest (stripe.vjs.example)
driver "oauth-poll"
SUBJECT = "vx.stripe.events"
BASE_URL = "https://api.stripe.com/v1"
AUTH_HEADER = "Bearer " + secret("stripe/api_key")
POLL_ENDPOINT = "/events"
POLL_INTERVAL = 60

// Flow 1: Process payment intents
// Subscribes to vx.stripe.events, filters type=payment_intent.succeeded

// Flow 2: Process subscriptions
// Subscribes to vx.stripe.events, filters type=customer.subscription.created

// Flow 3: Create payment (uses http-out to POST /v1/payment_intents)
```

## Admission Test Requirements (ADR-0017)

Every platform connector must pass admission tests:

1. **Manifest parses** (`vjs-check`)
2. **Credentials use `secret()`** (lint enforced)
3. **Golden fixture** (`fixture.json`) - sample output
4. **Mock server** (`mock.mjs`) - local API stand-in
5. **Probe passes** (`vejas_test_connector`)

## Platform Connector Catalog

### Planned HTTP-based Connectors

1. **Stripe** (oauth-poll) - Payments, Subscriptions, Customers, Invoices
2. **GitHub** (oauth-poll) - Issues, PRs, Repos, Webhooks
3. **Jira** (oauth-poll) - Issues, Projects, Workflows
4. **Twilio** (http-out) - SMS, Voice, WhatsApp
5. **SendGrid** (http-out) - Email sending
6. **Slack** (already exists) - Webhooks, API calls

### Planned Cloud Provider Connectors

7. **AWS** (http-out + SigV4) - S3, Lambda, SQS, SNS
8. **Google Cloud** (http-out + service account) - GCS, Pub/Sub, Functions
9. **Azure** (http-out + AD token) - Blob Storage, Service Bus, Functions

### Planned Database Connectors

10. **PostgreSQL** (exec-rpc) - Queries, transactions
11. **MongoDB** (exec-rpc) - CRUD operations
12. **Redis** (exec-rpc) - Commands, pub/sub
13. **Elasticsearch** (exec-rpc) - Index, search, bulk

### Existing Enterprise Connectors

- **SAP** (exec-rpc + exec-stream-source) - RFC calls, IDoc inbound
- **Salesforce** (native Rust) - SOQL, CRUD, streaming
- **IBM MQ** (native Rust) - Transactional messaging
- **AMQP** (native Rust) - RabbitMQ, etc.

## Anti-Pattern: Action-Based Connectors

❌ **Wrong**: One connector per action
- `aws_s3_upload` - only uploads to S3
- `twilio_sms_send` - only sends SMS
- `stripe_payment_intents_create` - only creates payment intents

✅ **Right**: One connector per platform
- `aws` - handles S3, Lambda, SQS, SNS operations
- `twilio` - handles SMS, Voice, WhatsApp operations
- `stripe` - handles all Stripe API operations

## Implementation Priority

**Phase 1: HTTP-based platforms** (use existing drivers)
1. Stripe (comprehensive)
2. GitHub
3. Twilio
4. Jira

**Phase 2: Cloud providers** (http-out + custom auth)
5. AWS
6. Google Cloud
7. Azure

**Phase 3: Databases** (exec-rpc wrappers)
8. PostgreSQL
9. MongoDB
10. Redis
11. Elasticsearch

Each connector must pass admission tests before being counted in the catalog.
