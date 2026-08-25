ARG GO_VERSION=1.26.7
ARG GO_IMAGE_DIGEST=sha256:8dc5cc7dbd10cbbd0840a63d2b17b3ea1b4dc297ab54e16b9b54fcd2cad87018
ARG NVIDIA_TOOLKIT_VERSION=1.19.1
ARG NVIDIA_TOOLKIT_IMAGE_DIGEST=sha256:c927adbc9b7755c5cb90022fdcc5c1295f5fe5fe1f38200a2dc65e85632b029c
ARG UBUNTU_IMAGE_DIGEST=sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517

FROM golang:${GO_VERSION}-bookworm@${GO_IMAGE_DIGEST} AS build
WORKDIR /src
COPY src/dra-driver/go.mod src/dra-driver/go.sum ./
RUN GOWORK=off go mod download
COPY src/dra-driver/ ./
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /dra-driver .

FROM nvcr.io/nvidia/k8s/container-toolkit:v${NVIDIA_TOOLKIT_VERSION}@${NVIDIA_TOOLKIT_IMAGE_DIGEST} AS nvidia

FROM ubuntu:24.04@${UBUNTU_IMAGE_DIGEST}
COPY --from=build /dra-driver /usr/local/bin/dra-driver
COPY --from=nvidia /artifacts/deb/usr/bin/nvidia-ctk /usr/bin/nvidia-ctk
COPY --from=nvidia /artifacts/deb/usr/bin/nvidia-cdi-hook /usr/bin/nvidia-cdi-hook
ENTRYPOINT ["/usr/local/bin/dra-driver"]
