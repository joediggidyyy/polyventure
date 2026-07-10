# Index

Back to [Guide Home](./README.md)

## Quick-reference and hosted surface

- One-page operator card: [OPERATOR_QUICK_REFERENCE.md](./OPERATOR_QUICK_REFERENCE.md)
- Hosted handbook shell: [web-pdf/index.html](./web-pdf/index.html)
- Contract execution plan: [UI_PROJECTION_CONTRACT_AUDIT_AND_CLI_EXECUTION_PLAN_2026-05-09.md](./UI_PROJECTION_CONTRACT_AUDIT_AND_CLI_EXECUTION_PLAN_2026-05-09.md)

## Authentication

- Headers and signing contract: [01-auth-and-signing.md](./01-auth-and-signing.md)
- REST path construction and query stripping: [01-auth-and-signing.md#rest-path-construction](./01-auth-and-signing.md#rest-path-construction)
- WebSocket handshake signing path `/trade-api/ws/v2`: [01-auth-and-signing.md#websocket-handshake-signing](./01-auth-and-signing.md#websocket-handshake-signing)
- Parse vs auth failures: [01-auth-and-signing.md#parse-failure-vs-auth-failure](./01-auth-and-signing.md#parse-failure-vs-auth-failure)
- Structured REST error envelope: [01-auth-and-signing.md#structured-rest-error-envelope](./01-auth-and-signing.md#structured-rest-error-envelope)
- Key lifecycle controls: [05-api-key-lifecycle-and-controls.md](./05-api-key-lifecycle-and-controls.md)

## Environments / Lanes

- Endpoint separation: [02-environments-and-lane-routing.md](./02-environments-and-lane-routing.md)
- Recommended vs compatibility hosts: [02-environments-and-lane-routing.md#environment-endpoints](./02-environments-and-lane-routing.md#environment-endpoints)
- Signing invariants across hosts: [02-environments-and-lane-routing.md#signing-invariants-across-hosts](./02-environments-and-lane-routing.md#signing-invariants-across-hosts)
- Runtime-source doctrine: [02-environments-and-lane-routing.md#runtime-source-doctrine](./02-environments-and-lane-routing.md#runtime-source-doctrine)
- Demo/live mismatch remediation: [07-troubleshooting-runbooks.md#runbook-c-demo-key-applied-to-live-or-reverse](./07-troubleshooting-runbooks.md#runbook-c-demo-key-applied-to-live-or-reverse)

## WebSockets

- Keepalive and channel model: [03-websocket-lifecycle-and-channels.md](./03-websocket-lifecycle-and-channels.md)
- Command-plane contract: [03-websocket-lifecycle-and-channels.md#command-plane-contract](./03-websocket-lifecycle-and-channels.md#command-plane-contract)
- Private execution channels: [03-websocket-lifecycle-and-channels.md#private-execution-channels-and-what-they-mean-operationally](./03-websocket-lifecycle-and-channels.md#private-execution-channels-and-what-they-mean-operationally)
- Orderbook channel constraints: [03-websocket-lifecycle-and-channels.md#orderbook-channel-constraints-and-recovery-hooks](./03-websocket-lifecycle-and-channels.md#orderbook-channel-constraints-and-recovery-hooks)
- Sequence integrity / snapshot recovery: [03-websocket-lifecycle-and-channels.md#sequence-integrity](./03-websocket-lifecycle-and-channels.md#sequence-integrity)
- Communications correlation: [03-websocket-lifecycle-and-channels.md#communications-correlation](./03-websocket-lifecycle-and-channels.md#communications-correlation)
- Direction fields and migration: [03-websocket-lifecycle-and-channels.md#direction-fields-and-migration-posture](./03-websocket-lifecycle-and-channels.md#direction-fields-and-migration-posture)
- `use_yes_price` handling: [03-websocket-lifecycle-and-channels.md#use_yes_price-handling](./03-websocket-lifecycle-and-channels.md#use_yes_price-handling)
- Transport vs application-flow classification: [03-websocket-lifecycle-and-channels.md#transport-vs-application-flow-classification](./03-websocket-lifecycle-and-channels.md#transport-vs-application-flow-classification)

## WebSocket commands and errors

- AsyncAPI reference: https://docs.kalshi.com/asyncapi.yaml
- Command family: `subscribe`, `unsubscribe`, `update_subscription`, `list_subscriptions`
- Error codes: 1–22 (see AsyncAPI `errorResponse` and `x-error-codes`)
- Evidence identifiers: `id`, `sid`, `seq` -> [Operator Quick Reference](./OPERATOR_QUICK_REFERENCE.md) and [Glossary](./GLOSSARY.md)

## Rate limits

- Budget model and flow: [04-rate-limits-and-throughput.md](./04-rate-limits-and-throughput.md)
- Tier budgets: [04-rate-limits-and-throughput.md#tier-budgets](./04-rate-limits-and-throughput.md#tier-budgets)
- Batch billing and endpoint overrides: [04-rate-limits-and-throughput.md#batch-billing-and-endpoint-overrides](./04-rate-limits-and-throughput.md#batch-billing-and-endpoint-overrides)
- Burst headroom: [04-rate-limits-and-throughput.md#burst-headroom](./04-rate-limits-and-throughput.md#burst-headroom)
- 429 storm remediation: [07-troubleshooting-runbooks.md#runbook-e-rate-limit-thrashing-429-storm](./07-troubleshooting-runbooks.md#runbook-e-rate-limit-thrashing-429-storm)

## Freshness and maintenance

- User data freshness watermark `/exchange/user_data_timestamp`: [07-troubleshooting-runbooks.md#runbook-g-rest-write-succeeded-but-follow-up-read-looks-stale](./07-troubleshooting-runbooks.md#runbook-g-rest-write-succeeded-but-follow-up-read-looks-stale)
- Maintenance / pause behavior and `cancel_order_on_pause`: [07-troubleshooting-runbooks.md#runbook-h-maintenance-window-or-exchange-pause-disrupts-expected-behavior](./07-troubleshooting-runbooks.md#runbook-h-maintenance-window-or-exchange-pause-disrupts-expected-behavior)
- Pricing-side interpretation terms: `outcome_side`, `book_side`, `taker_outcome_side`, `taker_book_side` in https://docs.kalshi.com/openapi.yaml

## Execution and reconciliation

- REST write vs private-stream disagreement: [07-troubleshooting-runbooks.md#runbook-i-rest-write-acknowledged-but-private-execution-streams-disagree-or-lag](./07-troubleshooting-runbooks.md#runbook-i-rest-write-acknowledged-but-private-execution-streams-disagree-or-lag)
- Order-group trigger / limit incidents: [07-troubleshooting-runbooks.md#runbook-j-order-group-limit-or-trigger-behavior-causes-unexpected-cancels-or-blocking](./07-troubleshooting-runbooks.md#runbook-j-order-group-limit-or-trigger-behavior-causes-unexpected-cancels-or-blocking)

## Project-specific implementation chapters

- Scope boundary: [README.md#guide-scope-and-chapter-boundaries](./README.md#guide-scope-and-chapter-boundaries)
- Polyventure integration map: [06-polyventure-integration-map.md](./06-polyventure-integration-map.md)
- Market-data foundation mapping: [06-polyventure-integration-map.md#market-data-foundation-mapping](./06-polyventure-integration-map.md#market-data-foundation-mapping)
- Execution/private-channel evidence mapping: [06-polyventure-integration-map.md#execution-and-private-channel-evidence-mapping](./06-polyventure-integration-map.md#execution-and-private-channel-evidence-mapping)
- Direction/pricing interpretation mapping: [06-polyventure-integration-map.md#direction-and-pricing-interpretation-mapping](./06-polyventure-integration-map.md#direction-and-pricing-interpretation-mapping)
- Polyventure owner-proof stack: [06-polyventure-integration-map.md#polyventure-owner-proof-stack](./06-polyventure-integration-map.md#polyventure-owner-proof-stack)
- Forensic evidence mapping: [06-polyventure-integration-map.md#forensic-evidence-mapping](./06-polyventure-integration-map.md#forensic-evidence-mapping)
- Polyventure-specific local-runtime runbooks: [07-troubleshooting-runbooks.md#polyventure-specific-local-runtime-runbooks](./07-troubleshooting-runbooks.md#polyventure-specific-local-runtime-runbooks)

## Public market data foundations

- Quick start market data: https://docs.kalshi.com/getting_started/quick_start_market_data
- Orderbook semantics: https://docs.kalshi.com/getting_started/orderbook_responses

## OpenAPI anchors

- Security schemes and headers: https://docs.kalshi.com/openapi.yaml
- Endpoint costs and limits: `GetAccountEndpointCosts`, `GetAccountApiLimits`
- API keys endpoints: `/api_keys`, `/api_keys/generate`, `/api_keys/{api_key}`

## Project-local code map

- Integration map chapter: [06-polyventure-integration-map.md](./06-polyventure-integration-map.md)
- Forensic evidence mapping: [06-polyventure-integration-map.md#forensic-evidence-mapping](./06-polyventure-integration-map.md#forensic-evidence-mapping)
- Primary files:
  - `src/polyventure/auth.py`
  - `src/polyventure/http_client.py`
  - `src/polyventure/config.py`
  - `src/polyventure/sandbox_preflight.py`
  - `src/polyventure/websocket_client.py`
  - `src/polyventure/web_app.py`
