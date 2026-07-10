# Operator Quick Reference — Kalshi API & WebSocket

Back to [Guide Home](./README.md) · Full diagnostics: [Troubleshooting Runbooks](./07-troubleshooting-runbooks.md)

> Purpose: fast first checks during live operations and incident response.

## Endpoint and signing crib sheet

| Lane    | Surface   | Recommended URL                                         | Also supported                                  |
| ------- | --------- | ------------------------------------------------------- | ----------------------------------------------- |
| Live    | REST      | `https://external-api.kalshi.com/trade-api/v2`          | `https://api.elections.kalshi.com/trade-api/v2` |
| Live    | WebSocket | `wss://external-api-ws.kalshi.com/trade-api/ws/v2`      | `wss://api.elections.kalshi.com/trade-api/ws/v2` |
| Sandbox | REST      | `https://external-api.demo.kalshi.co/trade-api/v2`      | `https://demo-api.kalshi.co/trade-api/v2`       |
| Sandbox | WebSocket | `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2`  | `wss://demo-api.kalshi.co/trade-api/ws/v2`      |

Signing invariant: host choice does not change the signed payload. Sign the full route from the API root without query parameters, for example `/trade-api/v2/portfolio/orders`, not the hostname and not `?limit=5`. WebSocket auth signs `/trade-api/ws/v2`.

## 1) First-60-seconds triage

1. Confirm selected lane (`sandbox` vs `live`).
2. Confirm selected host family matches the lane.
3. Confirm tuple: `(lane, api_key_id, private_key_path)`.
4. Confirm signing inputs: `timestamp_ms + METHOD + path_without_query`.
5. For WebSocket auth, confirm the signed path is `/trade-api/ws/v2`.
6. Confirm current failure class:
   - local parse/format issue
   - remote auth rejection
   - websocket transport/session issue
   - local application-runtime issue
   - snapshot/delta desync
   - pricing-mode confusion
   - 429/rate-limit pressure

   ```mermaid
   flowchart TD
      A[Incident starts] --> B[Confirm lane and host]
      B --> C[Confirm key tuple and signed path]
      C --> D{Failure class}
      D --> E[Parse or format issue]
      D --> F[Auth rejection]
      D --> G[WS session or seq gap]
      D --> H[Pricing mode issue]
      D --> I[429 pressure]
      E --> J[Runbook A]
      F --> K[Runbook B or C]
      G --> L[Runbook D]
      H --> M[Runbook F]
      I --> N[Runbook E]
   ```

## 2) Symptom → first checks

| Symptom                        | First check                                          | Escalate to                                                                                                             |
| ------------------------------ | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `format_error` / parse failure | Validate PEM and key path readability                | [Runbook A](./07-troubleshooting-runbooks.md#runbook-a-key-validation-fails-before-auth-check)                          |
| 401 on authenticated REST      | Recompute canonical signing path and method casing   | [Runbook B](./07-troubleshooting-runbooks.md#runbook-b-auth-fails-after-successful-signature-generation)                |
| Works in demo, fails in live   | Verify lane-key mismatch and probe both environments | [Runbook C](./07-troubleshooting-runbooks.md#runbook-c-demo-key-applied-to-live-or-reverse)                             |
| WS disconnect/reconnect loop   | Verify heartbeat ping/pong and resubscribe policy    | [Runbook D](./07-troubleshooting-runbooks.md#runbook-d-orderbook-delta-desync-or-sequence-gaps)                         |
| Burst of 429 responses         | Apply jittered backoff + concurrency caps            | [Runbook E](./07-troubleshooting-runbooks.md#runbook-e-rate-limit-thrashing-429-storm)                                  |
| Price fields look inverted     | Verify `use_yes_price` handling                      | [Runbook F](./07-troubleshooting-runbooks.md#runbook-f-pricing-scale-confusion-or-use_yes_price-mismatch)               |
| REST read looks stale          | Compare websocket truth and freshness watermark      | [Runbook G](./07-troubleshooting-runbooks.md#runbook-g-rest-write-succeeded-but-follow-up-read-looks-stale)             |
| Trading actions pause          | Check maintenance / exchange pause state             | [Runbook H](./07-troubleshooting-runbooks.md#runbook-h-maintenance-window-or-exchange-pause-disrupts-expected-behavior) |
| `websocket_service_unavailable` on sandbox; live healthy | WS handshake probe both sandbox hosts; confirm 503 from ELB — external outage, no config fix | [Runbook M](./07-troubleshooting-runbooks.md#runbook-m-sandbox-ws-connect-probe-fails-with-http-503-live-lane-healthy) |

## 3) Lane safety guardrail

Never promote staged credentials unless all three align:

- selected lane
- API key ID for that lane
- private key material for that lane

If any member mismatches, hold promotion and re-stage.

## 4) WebSocket health mini-check

- Connection authenticated successfully
- Subscriptions acknowledged (`subscribed`/`ok`)
- Heartbeat stable (ping/pong)
- Sequence continuity intact for snapshot/delta streams
- Last retained identifiers available: `id`, `sid`, `seq`, `market_ticker`, `ts_ms`

If sequence gap is detected, re-baseline from snapshot before applying deltas.

Important: a healthy websocket session does **not** by itself prove that the first authenticated account-scoped HTTP scan step will pass. Preserve transport truth and authenticated application-flow truth as separate checks.

## 5) Incident evidence minimum

Capture and retain:

- lane at time of failure
- host family in use
- REST path or WS channel in use
- REST status code or WebSocket error code
- command `id` where applicable
- subscription `sid` where applicable
- sequence number `seq` for stream incidents
- `market_ticker` or `market_id` when applicable
- `order_id` / `client_order_id` when applicable
- `ts_ms` or `as_of_time` when applicable
- short root-cause classification
- local artifact path tail when evidence was retained locally

Do not assume a generic exchange request-id header exists unless the specific surface documents one.

## 6) Kalshi market availability

Kalshi is not a traditional trading market. It operates **24 hours a day, 7 days a week**. The only scheduled maintenance window is **Thursday 3:00–5:00 AM local time** (operator's configured timezone).

Zero candidates returned by an active scan at any hour outside the Thursday window is not a market-availability issue. Investigate entry window filter settings (`entry_window_start_sec` / `entry_window_end_sec`), edge/profit thresholds, and current market conditions before concluding the exchange is unavailable.

As of 2026-06-19 (MFEW), the entry window is also enforced **server-side at fetch time**: the market fetch passes `min_close_ts` / `max_close_ts` to Kalshi so only markets closing inside the window are returned. The padding setting `entry_window_fetch_padding_sec` (default 15 s) reserves runtime for the fetch + processing before submit. A healthy in-window scan now shows `loaded_market_count` approximately equal to `entry_window_eligible_market_count` (both small), rather than `loaded=1000`. If `loaded_market_count` is unexpectedly zero, the server-side time bound returned nothing for the window — check the same filter/threshold settings, not exchange availability.

## 7) Deep links

- Auth signing rules: [01-auth-and-signing.md](./01-auth-and-signing.md)
- Lane routing model: [02-environments-and-lane-routing.md](./02-environments-and-lane-routing.md)
- WS lifecycle/channels: [03-websocket-lifecycle-and-channels.md](./03-websocket-lifecycle-and-channels.md)
- Rate limits: [04-rate-limits-and-throughput.md](./04-rate-limits-and-throughput.md)
- Key lifecycle controls: [05-api-key-lifecycle-and-controls.md](./05-api-key-lifecycle-and-controls.md)
- Evidence-first recovery steps: [07-troubleshooting-runbooks.md](./07-troubleshooting-runbooks.md)
- Term definitions: [GLOSSARY.md](./GLOSSARY.md)
