ARG GO_VERSION=1.26.7
ARG GO_IMAGE_DIGEST=sha256:8dc5cc7dbd10cbbd0840a63d2b17b3ea1b4dc297ab54e16b9b54fcd2cad87018
ARG DEBIAN_IMAGE_DIGEST=sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

FROM golang:${GO_VERSION}-bookworm@${GO_IMAGE_DIGEST} AS build
WORKDIR /src
COPY src/shared ./shared
COPY src/quantization-engine ./quantization-engine
WORKDIR /src/quantization-engine
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /quantization-engine .

FROM debian:bookworm-slim@${DEBIAN_IMAGE_DIGEST}
COPY --from=build /quantization-engine /usr/local/bin/quantization-engine
ENTRYPOINT ["/usr/local/bin/quantization-engine"]
