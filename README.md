# ChatGPT Dots

A BridgeV2 bridge that connects your existing ChatGPT Dot to Beeper and Matrix.
It uses native Dot messaging, preserving the Dot's identity, memory and tools
rather than creating a replacement chatbot.

Experimental and not yet available as a released Beeper network. This project
is unofficial and unaffiliated with OpenAI. Changes to ChatGPT's undocumented
protocol can interrupt the connection.

## Features

- Text messages, including incoming Markdown formatting.
- Images, files and videos in both directions, with a bridge-local 20 MiB limit.
- Task cards whose titles and status update in place.
- Incoming Dot emoji reactions, typing indicators and read receipts.
- Separate Dot and account avatars, account profile refresh and session renewal.
- Recovery of messages received after linking, including across restarts.

The bridge connects one existing primary Dot in a personal account. It does not
import regular ChatGPT conversations or pre-link history. Groups, creating Dots,
quoted replies, voice notes and sending reactions, typing or read receipts are
not supported. Some native message formats remain unsupported. See the
[setup guide](docs/README.md) to run a standalone instance.

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

[GNU GPL v3.0](LICENSE). Copyright (c) 2026 Beeper.
Dependencies retain their own licenses; see
[third-party notices](THIRD_PARTY_NOTICES.md).
