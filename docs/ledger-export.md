# Locally accepted ledger export

The optional ledger exporter sends a JSON document to an HTTP endpoint for each
ledger this node builds and accepts locally. It includes transactions in execution
order and complete state-entry changes. Export is disabled unless a URL is set.

```ini
[ledger_export]
url=https://receiver.example/ledgers
bearer_token=replace-with-your-token
# Optional; relative paths resolve under database_path.
outbox_path=ledger_export.sqlite
# Optional; default is 1 GiB of pending JSON payloads.
max_pending_bytes=1073741824
```

Omit `bearer_token` if the endpoint does not require bearer authentication. Keep
configuration files containing tokens private. URL user credentials, fragments,
and HTTP header control characters are rejected. HTTPS always verifies the peer
certificate and hostname, independently of the server's global `ssl_verify`
setting. The existing `ssl_verify_file` and `ssl_verify_dir` settings supply custom
trust roots when needed. HTTP is supported for receivers on trusted networks.

## Payload

Schema version 1 contains these fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | `1` |
| `network_id` | Node's network ID |
| `ledger_index`, `ledger_hash` | Accepted ledger's sequence and hash |
| `parent_ledger_hash` | Hash of its parent ledger |
| `close_time` | Seconds since the XRPL epoch (2000-01-01 UTC) |
| `parent_state_root`, `state_root` | Parent and accepted ledger state SHAMap roots |
| `stage` | `accepted`; this does not assert network validation |
| `pre_transaction_changes` | Direct ledger changes before transaction execution |
| `transactions` | Transactions sorted by metadata `TransactionIndex` |
| `post_transaction_changes` | Direct ledger changes after transaction execution |

Each transaction contains `hash`, `TransactionIndex`, `result`, `transaction`
(JSON), `transaction_blob`, `metadata_blob`, and `changes`. Inner batch transactions
also contain `parent_batch_id`. Blobs are hex-encoded canonical XRPL serialization.
Fee-claiming transactions include only the effects actually committed. Failed
attempts and rolled-back batch effects are excluded.

Each change contains `key` (the 256-bit ledger-entry key), `entry_type` (the numeric
XRPL ledger-entry type), `operation` (`insert`, `modify`, or `delete`), `before`, and
`after`. A present entry has the form `{"json": { ... }, "blob": "..."}`; an absent
entry is `null`. These are complete entries, including fields that transaction
metadata may omit. The blob is authoritative; the JSON is for inspection.

Changes are sorted by key and coalesced within each transaction or ledger phase.
The first before value and final after value are retained; net no-ops are omitted.
Changes to one key in different transactions remain separate. Metadata-generated
previous-transaction links are included. The ledger phases include Negative UNL
updates before transactions and skip-list updates after transactions.

## Reproducing a state root

The receiver must already hold the complete state SHAMap for
`parent_ledger_hash`, with root `parent_state_root`. Check the parent before
applying an export; locally accepted ledgers can form competing branches.

1. Apply `pre_transaction_changes`.
2. Apply each transaction's `changes` in ascending `TransactionIndex`.
3. Apply `post_transaction_changes`.
4. Check that the resulting state SHAMap root equals `state_root`.

For each change, verify the existing entry against `before.blob`, or its absence
when `before` is null. Delete the key when `after` is null; otherwise insert or
replace its leaf contents with the decoded `after.blob`. Use XRPL SHAMap state-leaf
and inner-node hashing. JSON serialization does not reproduce canonical leaf
bytes. Fees also change the ledger header, but that is separate from reproducing
the state SHAMap root.

The exporter does not supply a bootstrap snapshot, historical backfill, or data
for ledgers merely acquired from peers. Missing parents, downtime, and export gaps
require the receiver to obtain a suitable snapshot or recover through another
source. The exported ledger hash is not a validation proof.

## Delivery and recovery

Each complete payload is atomically stored in a dedicated SQLite outbox using
`synchronous=FULL` before an HTTP attempt. A separate worker sends one pending
payload at a time in persistent enqueue order, using:

- `Content-Type: application/json`
- `Idempotency-Key: <ledger_hash>`
- `Authorization: Bearer <bearer_token>`, when configured

HTTP 2xx acknowledges delivery. All other statuses and transport failures retry
indefinitely, with exponential delays from one to 60 seconds and a ten-second
timeout covering resolution, connection, TLS, request, and response headers.
Redirects are not followed. A failed first payload prevents later queued payloads
from overtaking it. Pending data resumes on restart, using the configured endpoint.
Changing that endpoint also changes the destination of existing queued payloads.

Delivery is **at least once**. A receiver may acknowledge a request just before
the sender crashes, causing it to be sent again. Deduplicate by `(network_id,
ledger_hash)` and acknowledge only after committing the received data.

The pending-byte limit measures JSON payload bytes, not SQLite files, indexes,
gap history, or WAL overhead. On overflow, capture failure, or outbox storage
failure, xrpld retains queued data, reports a gap, and continues ledger processing.
It never sends a partial payload. This means the exporter cannot guarantee a
continuous stream when its storage is exhausted or unavailable. Durable retries
apply after the outbox commit; a crash before persistence can also leave a gap.
Capture and serialization add CPU and memory costs, and committing the outbox adds
storage latency to ledger construction. HTTP waits occur only in the worker.

Admin `server_info` and `server_state` expose `ledger_export`, including `enabled`,
`storage_available`, `pending_ledgers`, `pending_bytes`, `last_acknowledged_ledger`,
`last_acknowledged_hash`, `last_error`, `gap_count`, and up to 100 gap details.
Unavailable storage metrics are omitted. Durable gap history remains in the
outbox database; gaps that cannot be persisted are reported in memory and logs.
Tokens, response bodies, and endpoint URLs are not logged by the exporter.

To disable export, remove the URL and restart. Retain the outbox if pending data
should be delivered when export is enabled again. Do not share one outbox between
running xrpld instances or different networks.
