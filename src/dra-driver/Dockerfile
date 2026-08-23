ARG GO_VERSION=1.26.7
ARG NVIDIA_TOOLKIT_VERSION=1.19.1

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY src/dra-driver/go.mod src/dra-driver/go.sum ./
RUN GOWORK=off go mod download
COPY src/dra-driver/ ./
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /dra-driver .

FROM nvcr.io/nvidia/k8s/container-toolkit:v${NVIDIA_TOOLKIT_VERSION} AS nvidia

FROM ubuntu:24.04
COPY --from=build /dra-driver /usr/local/bin/dra-driver
COPY --from=nvidia /artifacts/deb/usr/bin/nvidia-ctk /usr/bin/nvidia-ctk
COPY --from=nvidia /artifacts/deb/usr/bin/nvidia-cdi-hook /usr/bin/nvidia-cdi-hook
ENTRYPOINT ["/usr/local/bin/dra-driver"]
