ARG GO_VERSION=1.26.7
ARG GO_IMAGE_DIGEST=sha256:8dc5cc7dbd10cbbd0840a63d2b17b3ea1b4dc297ab54e16b9b54fcd2cad87018
ARG UBUNTU_IMAGE_DIGEST=sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517

FROM golang:${GO_VERSION}-bookworm@${GO_IMAGE_DIGEST} AS build
WORKDIR /src
COPY src/optical-dra-driver/go.mod src/optical-dra-driver/go.sum ./
RUN GOWORK=off go mod download
COPY src/optical-dra-driver/ ./
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /optical-dra-driver .

FROM ubuntu:24.04@${UBUNTU_IMAGE_DIGEST}
COPY --from=build /optical-dra-driver /usr/local/bin/optical-dra-driver
ENTRYPOINT ["/usr/local/bin/optical-dra-driver"]
