# Third-party notices

This project uses Go modules listed in `go.mod` and pinned by `go.sum`.
Dependency licenses remain applicable independently of this repository's
visibility or project license.

Direct dependencies:

| Module | License |
| --- | --- |
| `github.com/coder/websocket` | ISC |
| `github.com/yuin/goldmark` | MIT |
| `maunium.net/go/mautrix` | Mozilla Public License 2.0 |
| `go.mau.fi/util` | Mozilla Public License 2.0 |
| `github.com/google/uuid` | BSD 3-Clause |

The `goolm` build uses Mautrix's Go cryptography implementation rather than
requiring a system libolm installation. CGO remains necessary for SQLite.

The standard BridgeV2 command-line launcher also links
`maunium.net/go/mauflag` (GPL-3.0), a command-line argument parser.

Use `go mod download` and `go list -m -json all` to locate the exact dependency
sources and their license notices. [THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt)
collects upstream license and notice files for the modules linked by the pinned
standalone `goolm` build, along with the Go standard library license. Keep that
bundle current when dependencies change. Alternative build configurations can
require additional notices, including libolm when building without `goolm`.

The bridge's original source is licensed under [GNU GPL v3.0](LICENSE).
Dependencies retain their own terms. Binary distributors must preserve
applicable notices and provide corresponding source as required by those terms.
The repository, its pinned module graph and build scripts identify the source
inputs; CI archives and the container include the project and dependency notices.
