ARG GO_VERSION=1.26.7

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY src/shared ./shared
COPY src/quantization-engine ./quantization-engine
WORKDIR /src/quantization-engine
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /quantization-engine .

FROM debian:bookworm-slim
COPY --from=build /quantization-engine /usr/local/bin/quantization-engine
ENTRYPOINT ["/usr/local/bin/quantization-engine"]
