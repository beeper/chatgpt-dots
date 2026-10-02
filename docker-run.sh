#!/bin/sh
set -eu
umask 077

# UID is supplied by the container environment, not by the shell.
# shellcheck disable=SC3028
runtime_uid=${UID:-1337}
runtime_gid=${GID:-$runtime_uid}
case "$runtime_uid" in
    ''|*[!0-9]*) printf '%s\n' 'UID must be numeric.' >&2; exit 1 ;;
esac
case "$runtime_gid" in
    ''|*[!0-9]*) printf '%s\n' 'GID must be numeric.' >&2; exit 1 ;;
esac
if [ "$(id -u)" != 0 ] && { [ "$(id -u)" != "$runtime_uid" ] || [ "$(id -g)" != "$runtime_gid" ]; }; then
    printf '%s\n' 'Container user must match UID and GID.' >&2
    exit 1
fi
if [ "${1:-}" = --version ] || [ "${1:-}" = --help ]; then
    if [ "$(id -u)" = 0 ]; then
        exec su-exec "$runtime_uid:$runtime_gid" /usr/bin/chatgpt-dots "$@"
    fi
    exec /usr/bin/chatgpt-dots "$@"
fi

data_dir=${DATA_DIR:-/data}
config_path=${CONFIG_PATH:-$data_dir/config.yaml}
if [ ! -d "$data_dir" ] || [ -L "$data_dir" ] || [ "$(stat -c %a "$data_dir")" != 700 ] || [ "$(stat -c %u "$data_dir")" != "$runtime_uid" ]; then
    printf '%s\n' 'Mount an external runtime directory owned by UID with mode 0700.' >&2
    exit 1
fi
if [ ! -f "$config_path" ] || [ -L "$config_path" ] || [ "$(stat -c %a "$config_path")" != 600 ] || [ "$(stat -c %u "$config_path")" != "$runtime_uid" ]; then
    printf '%s\n' 'Provide an external config owned by UID with mode 0600.' >&2
    exit 1
fi
cd "$data_dir"
if [ "$(id -u)" = 0 ]; then
    exec su-exec "$runtime_uid:$runtime_gid" /usr/bin/chatgpt-dots -c "$config_path" "$@"
fi
exec /usr/bin/chatgpt-dots -c "$config_path" "$@"
