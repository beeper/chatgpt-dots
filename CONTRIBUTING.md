# Contributing

Use Go 1.27 and a C compiler. Follow the [build instructions](README.md#build),
format Go files with `gofmt`, and run:

```sh
go vet -tags goolm ./...
go test -tags goolm ./...
```

Keep native protocol code in `pkg/chatgpt` and Matrix mapping in `pkg/connector`.
Use existing BridgeV2 interfaces and patterns from other bridges.

Verify behavior changes in Beeper and ChatGPT with an authorized account,
including restart and recovery where relevant. Describe what you checked in
the pull request and update affected documentation. Keep credentials and
private messages out of contributions; see [Security](SECURITY.md).

Contributions are made under the project's [GNU GPL v3.0 license](LICENSE).
When updating dependencies, refresh their entries and upstream license texts in
`THIRD_PARTY_NOTICES.md` and `THIRD_PARTY_LICENSES.txt`.
