# Glossary

Back to [Guide Home](./README.md)

- **API key ID**: Identifier sent in `KALSHI-ACCESS-KEY`.
- **Canonical signing path**: Request path portion used in signature input, excluding query.
- **Command ID**: Client-supplied WebSocket command identifier carried in `id` and used to correlate subscribe/update/unsubscribe requests with responses.
- **Count FP**: Fixed-point contract quantity string (for example `"10.00"`).
- **Demo environment**: Kalshi test environment with separate credentials and endpoints.
- **Exchange pause**: Period where exchange or trading activity is paused, which can affect order behavior even when some surfaces remain reachable.
- **Event**: Collection of one or more markets.
- **Lane**: Local operator mode mapping to environment (`sandbox`/`live`).
- **Market**: Tradable binary/scalar contract object.
- **Market position update**: Private WebSocket update describing changes to your position in one or more markets after fills, settlements, or other position-affecting events.
- **Orderbook snapshot**: Full baseline state of orderbook levels.
- **Orderbook delta**: Incremental change event for one price level.
- **Order group**: Kalshi execution-control grouping that can auto-cancel or block grouped orders when its configured rolling limit is triggered.
- **Order group update**: Private WebSocket event describing order-group lifecycle or limit changes such as created, triggered, reset, deleted, or limit-updated.
- **Outcome side**: Directional exposure (`yes`/`no`).
- **Book side**: Book vocabulary (`bid`/`ask`) equivalent mapping to outcome side.
- **P95 latency**: 95th percentile observed request latency.
- **RSA-PSS**: Signature scheme used with SHA-256 for Kalshi API auth.
- **Sequence number**: Stream-ordering field `seq` used to detect gaps and trigger re-baselining for affected channels.
- **Series**: Collection of related events.
- **Subscription ID**: Server-assigned WebSocket subscription identifier carried in `sid` and used for unsubscribe or update operations.
- **Token bucket**: Rate-limit model with refill and capacity.
- **Trading pause**: Condition where trading activity is not currently permitted even if other exchange surfaces remain available.
- **`as_of_time`**: RFC3339 timestamp returned by `/exchange/user_data_timestamp`, used as an approximate freshness watermark for selected user-data REST endpoints.
- **`cancel_order_on_pause`**: Order option that cancels an open order if trading on the exchange is paused.
- **`use_yes_price`**: Pricing-mode setting or interpretation rule that determines whether orderbook and direction semantics are read in YES-price terms; mismatches can create apparent side or price inversions.
- **User data timestamp**: Freshness watermark returned by `/exchange/user_data_timestamp`, exposed as `as_of_time`, used to judge how current selected REST user-data endpoints are relative to recent writes and WebSocket events.

Additional official terminology: https://docs.kalshi.com/getting_started/terms
