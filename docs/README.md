# Standalone setup

For standalone development with Beeper. You'll need an existing ChatGPT Dot,
[bbctl](https://developers.beeper.com/bridges), and a bridge binary built using
the [build instructions](../README.md#build).

## Register

Use a different registration name if `sh-chatgpt-dots` is already in use.

```sh
umask 077
mkdir -p "$HOME/chatgpt-dots-data"
bbctl --env prod whoami
bbctl --env prod config --type bridgev2 --param pickle_key=generate \
  -o "$HOME/chatgpt-dots-data/config.yaml" sh-chatgpt-dots
```

## Configure

Set these fields in the generated configuration, keeping its registration
tokens and Matrix settings:

```yaml
network: {}
bridge:
    split_portals: true
    async_events: false
    portal_event_buffer: 0
backfill:
    enabled: false
    queue:
        enabled: false
provisioning:
    debug_endpoints: false
logging:
    min_level: info
```

## Run and connect

```sh
cd "$HOME/chatgpt-dots-data"
/absolute/path/to/chatgpt-dots -c ./config.yaml
```

Add the bridge through Beeper's self-hosted account setup and sign in with
ChatGPT. Your existing Dot's room appears after linking. Keep the same
configuration and database when updating the executable.

For Docker, build the [image](../Dockerfile), mount the runtime directory at
`/data`, and set `UID` and `GID` to its owner's IDs.

[Features and limitations](../README.md#features) ·
[Architecture](architecture.md) · [Security](../SECURITY.md)
