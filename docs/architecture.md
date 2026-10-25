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
separate. The account subtitle uses the authenticated email. Avatars refresh on
connect and every 30 minutes.

## Messages and recovery

Linking records a baseline without importing earlier history. REST polling
fetches new messages and rechecks recent ones every 3 seconds, or every 30
seconds while the native WebSocket is connected. A full scan of post-link
history runs on connect and every 30 minutes, so edits and reactions on older
messages can take that long to appear. Normal polls skip unchanged recent
messages. Full scans reconcile every post-link message, including task cards
whose status can change without changing their attachment message.

Native message IDs and stable attachment-part IDs map to Matrix events.
Delivery is synchronous, with checkpoints saved after all parts are persisted.
Retries reuse completed parts. Outgoing request IDs derive from Matrix events,
allowing uncertain sends to be checked before retrying.

Native `reply_to.message_id` and `reply_root_message_id` become Matrix reply and
thread relationships. Outgoing replies send only the native parent message ID;
ChatGPT determines the reply root. Replies can include text or media and target
either participant's messages. Retry reservations include the reply target, and
an uncertain reply is reconciled through the native reply history before another
send is attempted. Targets must belong to the same bridged account and room.

Text is rendered as safe Matrix HTML. Media uses native room uploads and Matrix
media transfer, with a 20 MiB bridge limit. Task attachments become cards whose
title and status update in place, not Matrix threads. Cards refresh every few
seconds while changing and back off while unchanged. After a successful refresh
confirms a task has been finished for an hour, its card leaves frequent polling.
Full scans still refresh it to recover later resumes.

## Native activity

The native WebSocket supplies typing heartbeats and triggers reconciliation for
verified Dot rooms. Typing expires rather than being replayed after reconnect.
Read receipts use the Dot's native read cursor and timestamp, not inferred
activity. Reaction snapshots reconcile independently of message content.
Typing and read receipts are incoming-only. Both participants' reactions are
synced, including the owner's reactions made in ChatGPT. The native protocol
allows one emoji per participant per message: adding another replaces it, and
removing it uses the native reaction DELETE endpoint. Before removing a reaction,
the bridge checks that it still matches the native one, so removing an older
Matrix reaction does not clear an already-observed replacement.

Reply and reaction request shapes follow the native web client's
[messaging implementation](https://chatgpt.com/cdn/assets/async/963553.432cebca7f.js),
inspected on October 25, 2026. The emoji picker accepts emoji beyond its quick
suggestions; the bridge does not restrict reactions to that suggestion list.

## Confirmations

Native `content.elicitation` requests remain attached to their original message.
The standard BridgeV2 command processor handles `!dots accept <code>`, `!dots decline <code>`
and `!dots check <code>` in the chat without requiring quoted replies. The code
binds the request definition to its persisted message in that account and room.
Before submitting a decision the
connector verifies the account, room members, original Dot sender, request
definition and provider's current request fingerprint. Only confirmations
with no form fields or one native confirmation checkbox can be
answered from Beeper; other requests link to the native ChatGPT interface.

The login metadata reserves the decision before submission. Only a provider
`applied` response is reported as confirmation. An ambiguous response remains
reserved across restart and can be inspected without repeating the POST.

See the [README](../README.md) for supported features and limitations.
