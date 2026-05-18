# Notes

## Assumptions

- One on-chain transaction hash can grant inventory at most once across the system.
- `MOCK_CHAIN=true` is acceptable for local and CI flows; live RPC verification is enabled by setting `MOCK_CHAIN=false` and `RPC_URL`.
- A claim that cannot find a receipt after the configured retry window is marked failed with `receipt_not_found`. In a production queue, I would usually re-enqueue with a longer backoff instead of failing immediately.
- The starter socket queue can deliver duplicate jobs, so database writes must be idempotent.

## What Changed

- Inventory grants are now insert-once effects. If the same `source_tx_hash` is processed again, the worker skips the duplicate inventory write instead of incrementing quantity.
- The schema includes a unique partial index on `inventory_items.source_tx_hash` for non-null transaction hashes.
- Claim logs now carry correlation fields across HTTP, DB row creation, queue enqueue, worker receipt, verification, inventory grant, and final status.

## RPC / Reorg Tradeoffs

- The live verifier polls `getTransactionReceipt` with bounded retries.
- Reverted transactions are failed immediately.
- Missing receipts are failed after retries in this exercise to keep behavior deterministic and easy to test.
- This does not fully protect against chain reorgs. With more time, I would require a configurable confirmation depth, re-check the block hash before finalizing, and treat shallow confirmations as retryable instead of terminal.

## What I Would Do Next

- Replace the local socket queue with a durable queue that supports visibility timeouts, delayed retries, and a dead-letter queue.
- Add a `claim_events` table for an auditable status timeline.
- Add metrics for claim latency, verification failures, duplicate job deliveries, and queue depth.
- Add endpoint-level integration tests that run the API and worker together.

## CI Testing Plan

- Run `npm ci`.
- Run `npm test` with embedded PGlite and `MOCK_CHAIN=true`.
- Add a smoke test for `POST /users`, `POST /credits`, `POST /claims`, polling claim status, and checking inventory.
- In a separate scheduled or gated job, run live RPC tests with a non-secret public RPC URL and known transaction hashes.
