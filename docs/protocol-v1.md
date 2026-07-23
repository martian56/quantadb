# QuantaDB protocol v1

Protocol v1 is a temporary control protocol for developing the core database.

The server also speaks the PostgreSQL v3 wire protocol on its own listener,
`127.0.0.1:55432` by default (`QUANTA_PG_LISTEN_ADDRESS`, `off` disables it).
The simple query protocol is supported: startup, SSL negotiation with a
polite refusal, trust authentication, `Query`, transactions, and errors with
SQLSTATE codes, which is enough for psql and drivers that avoid prepared
statements. The extended query protocol answers with a clear error until it
is implemented. Result columns map to the `bool`, `int8`, `float8`, and
`text` OIDs in text format.

## Framing

Connections use TCP. Each frame is one UTF-8 JSON object followed by `\n`.
`\r\n` is accepted. Frames are rejected before their buffered payload exceeds
the configured maximum.

The server sends exactly one response for each accepted request and never
sends an unsolicited welcome frame.

## Envelope

```json
{
  "protocol_version": 1,
  "request_id": 42,
  "request": {
    "type": "ping"
  }
}
```

`request_id` is chosen by the client and copied to the response. Clients can
therefore correlate responses without relying on response content.

## Requests

### `ping`

Returns `pong` and the server package version.

### `parse`

```json
{
  "protocol_version": 1,
  "request_id": 43,
  "request": {
    "type": "parse",
    "sql": "SELECT id FROM users"
  }
}
```

Returns the span-aware AST or a structured `syntax_error`.

### `execute`

```json
{
  "protocol_version": 1,
  "request_id": 44,
  "request": {
    "type": "execute",
    "sql": "SELECT id, balance FROM accounts WHERE balance > 0"
  }
}
```

Returns one structured result per statement: transaction state, command tag
and affected-row count, or typed query columns and rows. Transaction state is
connection-scoped, so `BEGIN`, later execute frames, and `COMMIT` share one
snapshot. Engine and transaction failures use `execution_error` and
`transaction_error`.

## Errors

Protocol errors have stable machine-readable codes:

- `invalid_json`
- `unsupported_protocol_version`
- `syntax_error`
- `execution_unavailable`
- `execution_error`
- `transaction_error`
- `frame_too_large`
- `idle_timeout`
- `server_busy`

Syntax errors include an optional half-open byte span. Errors that occur before
a valid request envelope is decoded use request ID `0`.

Validated requests pass through a separate bounded execution admission limit.
When all service slots are occupied, the server returns `server_busy` without
queuing unbounded work. The default is 256 in-flight requests and can be
changed with `QUANTA_MAX_IN_FLIGHT_REQUESTS`.
