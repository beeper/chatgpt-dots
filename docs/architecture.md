# Architecture

A Go connector that maps one existing ChatGPT Dot conversation to Matrix using
mautrix BridgeV2. It runs standalone or as a module in Megabridge.

## Ownership

| Component | Responsibility |
| --- | --- |
| `cmd/chatgpt-dots` | Standalone startup through `mxmain.BridgeMain`. |
| `pkg/chatgpt` | Native authentication, Dot discovery, messaging, task metadata and media. |
| `pkg/connector` | Login, identity, Matrix conversion and delivery checkpoints. |
| BridgeV2 | Provisioning, event mappings, database and Matrix encryption. |
| Megabridge / Megamanager | Cloud hosting and lifecycle. |
| Services / SDK / clients | Network availability, account setup and presentation. |

The cloud network ID is `chatgpt-dots`. Standalone development uses a separate
self-hosted registration; there is no embedded/on-device connector.

## Identity

The standard cookie login flow obtains renewable ChatGPT credentials. Account,
Dot, room and member IDs are verified before messaging; reauthentication keeps
the existing mappings. Credential scope and storage are covered in
[Security](../SECURITY.md).

The Dot's avatar, authenticated user's profile and client network icon are
separate. The account subtitle uses the authenticated email.

## Messages and recovery

Linking records a baseline without importing earlier history. REST polling
fetches new messages; periodic scans and restart reconciliation recover
post-link changes.

Native message IDs and stable attachment-part IDs map to Matrix events.
Delivery is synchronous, with checkpoints saved after all parts are persisted.
Retries reuse completed parts. Outgoing request IDs derive from Matrix events,
allowing uncertain sends to be checked before retrying.

Text is rendered as safe Matrix HTML. Media uses native room uploads and Matrix
media transfer, with a 20 MiB bridge limit. Task attachments become cards whose
title and status update in place, not Matrix threads.

## Native activity

The native WebSocket supplies typing heartbeats and triggers reconciliation for
verified Dot rooms. Typing expires rather than being replayed after reconnect.
Read receipts use the Dot's native read cursor and timestamp, not inferred
activity. Reaction snapshots reconcile independently of message content.
These signals are incoming-only.

See the [README](../README.md) for supported features and limitations.
