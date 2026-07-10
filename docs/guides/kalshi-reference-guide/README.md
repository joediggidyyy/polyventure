# Kalshi API & WebSocket Operator Reference

This guide is a cross-linked, local-first reference for operating against the Kalshi API and WebSocket surfaces.

It is both:

- an operator handbook for day-to-day Kalshi integration work, and
- an evidence-capture reference for incident review and recovery.

Reviewed public Kalshi docs do not establish a generic REST request-id or correlation-id response-header contract for arbitrary incidents. Operators should therefore anchor incident evidence in documented identifiers such as command ids, subscription ids, sequence numbers, order ids, client order ids, market identifiers, and timestamp fields, plus implementation-local artifacts where retained.

## How to use this guide

- Start at [Operator Quick Reference](./OPERATOR_QUICK_REFERENCE.md) for first-response triage.
- Start at [Auth & Signing](./01-auth-and-signing.md) if you are validating keys or debugging signatures.
- Start at [Environments & Lane Routing](./02-environments-and-lane-routing.md) if you are validating demo/live separation.
- Start at [WebSocket Lifecycle](./03-websocket-lifecycle-and-channels.md) if you are troubleshooting stream connectivity.
- Start at [Troubleshooting Runbooks](./07-troubleshooting-runbooks.md) for issue-driven workflows.
- Use [Index](./INDEX.md) for field and endpoint lookup.
- Use [Glossary](./GLOSSARY.md) when a term appears in the runbooks or quick reference but needs a compact definition.

## Guide scope and chapter boundaries

The beginning and bulk of this handbook are intentionally Kalshi-general.

Use the corpus in this order:

1. chapters `01` through `05`, plus the glossary and index, define general Kalshi API and WebSocket behavior,
2. chapter `06` isolates Polyventure runtime mapping and other project-local implementation surfaces,
3. the Polyventure-specific local-runtime runbooks are grouped at the end of chapter `07`,
4. the UI projection plan is a project-local supporting artifact rather than part of the general Kalshi API reference.

Guide boundary:

- use this guide for exchange/protocol truth and stable operating doctrine,
- use project-local implementation chapters only when the issue depends on a specific runtime,
- keep project-local evidence registers separate from the general Kalshi API reference.

## Book map

0. [Operator Quick Reference (one-page)](./OPERATOR_QUICK_REFERENCE.md)
1. [Auth & Signing](./01-auth-and-signing.md)
2. [Environments & Lane Routing](./02-environments-and-lane-routing.md)
3. [WebSocket Lifecycle & Channels](./03-websocket-lifecycle-and-channels.md)
4. [Rate Limits & Throughput Design](./04-rate-limits-and-throughput.md)
5. [API Key Lifecycle & Operational Controls](./05-api-key-lifecycle-and-controls.md)
6. [Polyventure Integration Map](./06-polyventure-integration-map.md)
7. [Troubleshooting Runbooks](./07-troubleshooting-runbooks.md)
8. [Glossary](./GLOSSARY.md)
9. [Index](./INDEX.md)
10. [UI Projection Contract Audit + CLI Execution Plan (2026-05-09)](./UI_PROJECTION_CONTRACT_AUDIT_AND_CLI_EXECUTION_PLAN_2026-05-09.md)

## Web-hosted HTML edition

- Hosted handbook shell: [web-pdf/index.html](./web-pdf/index.html)
- One-page operator handout: [OPERATOR_QUICK_REFERENCE.md](./OPERATOR_QUICK_REFERENCE.md)

This path provides a hosted HTML handbook shell with a fixed header, a fixed chapter-tile banner, and a scrollable content viewport. The same web edition can be printed to PDF when a static export artifact is needed.

## Primary source set

- Kalshi Docs index: https://docs.kalshi.com/llms.txt
- API environments: https://docs.kalshi.com/getting_started/api_environments
- Demo environment: https://docs.kalshi.com/getting_started/demo_env
- API keys: https://docs.kalshi.com/getting_started/api_keys
- WebSockets: https://docs.kalshi.com/websockets
- Keep-alive: https://docs.kalshi.com/websockets/connection-keep-alive
- Rate limits: https://docs.kalshi.com/getting_started/rate_limits
- OpenAPI: https://docs.kalshi.com/openapi.yaml
- AsyncAPI: https://docs.kalshi.com/asyncapi.yaml

## Navigation aides

- Concept definitions: [Glossary](./GLOSSARY.md)
- Endpoint-and-topic lookup: [Index](./INDEX.md)
- Project-local implementation tie-in: [Polyventure Integration Map](./06-polyventure-integration-map.md)
- Incident recovery workflows: [Troubleshooting Runbooks](./07-troubleshooting-runbooks.md)
