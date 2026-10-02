ARG GO_IMAGE=golang:1.27-alpine3.23@sha256:d9e2f2f07b10cc922da3e80e035c3058810b328d5aef82d2c63680967c5e2ec9
ARG RUNTIME_IMAGE=alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM ${GO_IMAGE} AS builder
ENV CGO_ENABLED=1 GOTOOLCHAIN=local
RUN apk add --no-cache git ca-certificates build-base
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY build.sh ./
COPY cmd ./cmd
COPY pkg ./pkg
ARG CI_COMMIT_SHA=unknown
ARG CI_COMMIT_TAG=unknown
ARG CI_BUILD_TIME
RUN sh ./build.sh && ./chatgpt-dots --version

FROM ${RUNTIME_IMAGE} AS runtime
ENV UID=1337 GID=1337
RUN apk add --no-cache su-exec ca-certificates
COPY --from=builder --chmod=0755 /build/chatgpt-dots /usr/bin/chatgpt-dots
COPY --chmod=0755 docker-run.sh /docker-run.sh
COPY LICENSE THIRD_PARTY_NOTICES.md THIRD_PARTY_LICENSES.txt /usr/share/licenses/chatgpt-dots/
WORKDIR /data
VOLUME /data
ENTRYPOINT ["/docker-run.sh"]
