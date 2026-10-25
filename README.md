# ChatGPT Dots

A BridgeV2 bridge that connects your existing ChatGPT Dot to Beeper and Matrix.
It uses native Dot messaging, preserving the Dot's identity, memory and tools
rather than creating a replacement chatbot.

Experimental and not yet available as a released Beeper network. This project
is unofficial and unaffiliated with OpenAI. Changes to ChatGPT's undocumented
protocol can interrupt the connection.

## Features

- Text messages, including incoming Markdown formatting.
- Quoted replies and native reply threads in both directions.
- Emoji reactions in both directions, with one reaction per person per message.
- Images, files and videos in both directions, with a bridge-local 20 MiB limit.
- Task cards whose titles and status update in place.
- Confirmation cards with explicit accept, decline and status commands.
- Incoming typing indicators and read receipts.
- Separate Dot and account avatars, account profile refresh and session renewal.
- Recovery of messages received after linking, including across restarts.

The bridge connects one existing primary Dot in a personal account. It does not
import regular ChatGPT conversations or pre-link history. Groups, creating Dots,
voice notes and sending typing or read receipts are
not supported. Some native message formats remain unsupported. See the
[setup guide](docs/README.md) to run a standalone instance.

When a Dot asks for confirmation, the card includes a command such as
`!dots accept a1b2c3d4e5f6`. Send that command in the chat to accept, or use
`!dots decline a1b2c3d4e5f6` to decline. `!dots check a1b2c3d4e5f6` reads its
status. No quoted reply is needed. Ordinary messages such as “Yes” stay chat
messages. Requests that need other form fields, sign-in, location
or verification direct you to ChatGPT. A decision with an uncertain outcome
is never automatically submitted again, including after a restart.

## Build

Use Go 1.27 and a C compiler. Linux and macOS are supported:

```sh
./build.sh
./chatgpt-dots --version
go vet -mod=readonly -tags goolm ./...
```

The build uses Go cryptography and CGO SQLite; no system libolm is required.
A [Dockerfile](Dockerfile) is also provided. Keep configuration, databases and
credentials outside the checkout and container image.

## Security

ChatGPT sign-in provides credentials with broader account access, **not a
provider-enforced Dots-only grant**. The connector limits their use to
authentication, Dot discovery, verified Dot-room operations and metadata for
tasks attached to those rooms.

## Documentation

[Standalone setup](docs/README.md) · [Architecture](docs/architecture.md) ·
[Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

## License

[GNU AGPL v3.0](LICENSE). Copyright (c) 2026 Beeper.
Dependencies retain their own licenses; see
[third-party notices](THIRD_PARTY_NOTICES.md).
