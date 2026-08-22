ARG GO_VERSION=1.26.7
ARG SLURM_BASE_IMAGE=slurm-control-plane:dev
ARG OPENTPU_REVISION=ca5d381c93752a504e8de1fa10c9d51649853c70
ARG PYRTL_VERSION=0.12
ARG NUMPY_VERSION=2.3.3

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY src/slurm-compute-node/go.mod src/slurm-compute-node/go.sum ./
RUN GOWORK=off go mod download
COPY src/slurm-compute-node/ ./
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /gres-init ./cmd/gres-init

FROM ${SLURM_BASE_IMAGE}
ARG OPENTPU_REVISION
ARG PYRTL_VERSION
ARG NUMPY_VERSION
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends git python3 python3-pip && \
    git clone https://github.com/UCSBarchlab/OpenTPU.git /opt/opentpu && \
    git -C /opt/opentpu checkout "${OPENTPU_REVISION}" && \
    rm -rf /opt/opentpu/.git && \
    python3 -m pip install --break-system-packages --no-cache-dir "pyrtl==${PYRTL_VERSION}" "numpy==${NUMPY_VERSION}" && \
    cd /opt/opentpu && python3 assembler.py simplemult.a && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /gres-init /usr/local/bin/gres-init
COPY src/python-workloads/opentpu-harness/runtime.py /usr/local/bin/opentpu-runtime.py
COPY src/slurm-compute-node/cuda-smoke.py /usr/local/bin/cuda-smoke.py
ENV PYTHONPATH=/opt/opentpu
