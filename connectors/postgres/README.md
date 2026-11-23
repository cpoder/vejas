# PostgreSQL Connector (exec-rpc)

Standalone Rust binary for PostgreSQL database operations via NATS request/reply.

## Architecture

- **Driver**: `exec-rpc` (long-running request/reply process)
- **Protocol**: JSON requests on stdin → JSON responses on stdout
- **Subject**: `vxrpc.postgres` (outside vx.* JetStream)
- **Binary**: `vejas-postgres` (compiled from Rust)

## Operations

### 1. Ping
Test connectivity:
```json
{"op": "ping"}
```
Response:
```json
{"ok": true, "result": {"pong": true}}
```

### 2. Query
Execute SELECT queries:
```json
{
  "op": "query",
  "sql": "SELECT * FROM users WHERE id = $1",
  "params": [42]
}
```
Response:
```json
{
  "ok": true,
  "result": [
    {"id": 42, "name": "Alice", "email": "alice@example.com"}
  ]
}
```

### 3. Execute
Execute INSERT/UPDATE/DELETE:
```json
{
  "op": "execute",
  "sql": "INSERT INTO users (name, email) VALUES ($1, $2)",
  "params": ["Bob", "bob@example.com"]
}
```
Response:
```json
{
  "ok": true,
  "result": {"affected_rows": 1}
}
```

### 4. Transaction
Execute multiple statements atomically:
```json
{
  "op": "transaction",
  "statements": [
    {
      "sql": "INSERT INTO users (name) VALUES ($1)",
      "params": ["Charlie"]
    },
    {
      "sql": "UPDATE accounts SET balance = balance - $1 WHERE user_id = $2",
      "params": [100, 1]
    }
  ]
}
```
Response:
```json
{
  "ok": true,
  "result": {
    "results": [
      {"affected_rows": 1},
      {"affected_rows": 1}
    ]
  }
}
```

## Configuration

### Manifest: `connectors/postgres_rpc.vjs`

```javascript
driver "exec-rpc"
REQUEST_SUBJECT = "vxrpc.postgres"
CMD = "./connectors/postgres/target/release/vejas-postgres"
ENV = {
    "POSTGRES_URL": secret("postgres/url")
}
```

### Secret Setup

```bash
# Set PostgreSQL connection URL
vejas_set_secret postgres/url "postgresql://user:password@localhost:5432/dbname"

# Or use DATABASE_URL (fallback)
vejas_set_secret postgres/url "postgres://user:password@localhost/dbname"
```

Connection string format:
```
postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&...]
```

## Security (ADR-0008)

- ✅ Credentials via ENV variables only (never argv)
- ✅ Connection string in secrets vault
- ✅ Process isolation (connector crash ≠ runtime crash)
- ✅ No credentials in /proc/*/cmdline

## Type Mapping

| PostgreSQL Type | JSON Type |
|----------------|-----------|
| int2, int4 | number (integer) |
| int8 | number (integer) |
| float4, float8 | number (float) |
| bool | boolean |
| text, varchar, char | string |
| json, jsonb | object/array |
| NULL | null |

## Building

```bash
cd connectors/postgres
cargo build --release
# Binary: target/release/vejas-postgres (optimized, stripped)
```

## Usage from Flows

### Query Example
```javascript
// Flow that queries PostgreSQL
source "vx.trigger"

transform {
    // Call PostgreSQL via RPC
    let request = {
        "op": "query",
        "sql": "SELECT * FROM orders WHERE status = $1",
        "params": ["pending"]
    }
    
    // Send request to vxrpc.postgres
    let response = rpc("vxrpc.postgres", request)
    
    if response.ok {
        emit("vx.orders.pending", response.result)
    }
}
```

### Transaction Example
```javascript
// Flow that executes a transaction
source "vx.payment.received"

transform {
    let request = {
        "op": "transaction",
        "statements": [
            {
                "sql": "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
                "params": [event.amount, event.account_id]
            },
            {
                "sql": "INSERT INTO transactions (account_id, amount, type) VALUES ($1, $2, $3)",
                "params": [event.account_id, event.amount, "credit"]
            }
        ]
    }
    
    let response = rpc("vxrpc.postgres", request)
    
    if response.ok {
        emit("vx.payment.processed", {"success": true})
    } else {
        emit("vx.payment.failed", {"error": response.error})
    }
}
```

## Error Handling

All errors return:
```json
{
  "ok": false,
  "error": "descriptive error message"
}
```

Common errors:
- Connection failures: "Failed to connect to PostgreSQL: ..."
- Query errors: "query error: ..."
- Transaction errors: "transaction error: ..." (auto-rollback)
- Invalid JSON: "invalid request JSON: ..."

## Performance

- **Connection pooling**: Single persistent connection per connector instance
- **Binary size**: ~2.5MB (stripped, optimized)
- **Latency**: Request/reply over NATS (~1-5ms overhead)
- **Concurrency**: Requests serialized per connector instance
- **Clustering**: Multiple instances share load via queue group (ADR-0020)

## Testing

```bash
# Start PostgreSQL (Docker)
docker run -d --name postgres-test \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  postgres:16-alpine

# Set secret
vejas_set_secret postgres/url "postgresql://postgres:test@localhost:5432/testdb"

# Test ping
echo '{"op":"ping"}' | ./target/release/vejas-postgres

# Test query
echo '{"op":"query","sql":"SELECT version()"}' | ./target/release/vejas-postgres
```

## Limitations

- One connection per connector instance (serialized requests)
- No connection pooling within the binary (use multiple instances for concurrency)
- No prepared statement caching (each request is independent)
- Large result sets load fully into memory (paginate in SQL if needed)

## Related

- ADR-0011: Connector Extensibility (exec-process architecture)
- ADR-0008: Secrets Vault (no credentials in argv)
- ADR-0020: Clustering (queue groups for load balancing)
