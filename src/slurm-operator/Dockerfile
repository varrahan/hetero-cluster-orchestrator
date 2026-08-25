ARG GO_VERSION=1.26.7
ARG GO_IMAGE_DIGEST=sha256:8dc5cc7dbd10cbbd0840a63d2b17b3ea1b4dc297ab54e16b9b54fcd2cad87018
ARG UBUNTU_IMAGE_DIGEST=sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517
ARG SLURM_VERSION=26.05.3
ARG SLURM_SHA256=f4219b15b8e4e8dc2559052abfd760759598867cd28051896d3b09c0a06a2ab7

FROM golang:${GO_VERSION}-bookworm@${GO_IMAGE_DIGEST} AS operator-build
WORKDIR /src
COPY src/shared /shared
COPY src/slurm-operator/go.mod src/slurm-operator/go.sum ./
RUN GOWORK=off go mod download
COPY src/slurm-operator/ ./
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /slurm-operator .

FROM scratch AS operator
COPY --from=operator-build /slurm-operator /slurm-operator
USER 65532:65532
ENTRYPOINT ["/slurm-operator"]

FROM ubuntu:24.04@${UBUNTU_IMAGE_DIGEST} AS slurm-build
ARG SLURM_VERSION
ARG SLURM_SHA256
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential bzip2 ca-certificates curl pkg-config python3 \
    libdbus-1-dev libhttp-parser-dev libjson-c-dev libjwt-dev liblz4-dev \
    libmariadb-dev libmunge-dev libncurses-dev libpam0g-dev libreadline-dev \
    libyaml-dev libzstd-dev && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN curl --fail --location --output slurm.tar.bz2 \
      "https://download.schedmd.com/slurm/slurm-${SLURM_VERSION}.tar.bz2" && \
    echo "${SLURM_SHA256}  slurm.tar.bz2" | sha256sum --check --strict && \
    tar --extract --bzip2 --file slurm.tar.bz2 --strip-components=1 && \
    ./configure --prefix=/usr --sysconfdir=/etc/slurm --localstatedir=/var \
      --with-http-parser --with-jwt --with-munge && \
    make -j"$(nproc)" && \
    make install DESTDIR=/install

FROM ubuntu:24.04@${UBUNTU_IMAGE_DIGEST} AS slurm-control-plane
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates libdbus-1-3 libhttp-parser2.9 libjson-c5 libjwt2 liblz4-1 \
    libmariadb3 libmunge2 libncurses6 libpam0g libreadline8t64 libyaml-0-2 \
    libzstd1 munge && \
    groupadd --gid 64030 slurm && \
    useradd --uid 64030 --gid slurm --home-dir /var/lib/slurm --create-home --shell /bin/bash slurm && \
    groupadd --gid 64031 slurmrestd && \
    useradd --uid 64031 --gid slurmrestd --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin slurmrestd && \
    install -d -m 0755 /run/secrets/slurm && \
    install -d -o slurm -g slurm /var/lib/slurmctld /var/lib/slurmd && \
    rm -f /etc/munge/munge.key && \
    rm -rf /var/lib/apt/lists/*
COPY --from=slurm-build /install/ /
