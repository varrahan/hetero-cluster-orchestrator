ARG GO_VERSION=1.26.7
ARG GO_IMAGE_DIGEST=sha256:8dc5cc7dbd10cbbd0840a63d2b17b3ea1b4dc297ab54e16b9b54fcd2cad87018
ARG SLURM_BASE_IMAGE=slurm-control-plane:dev
ARG OPENTPU_REVISION=ca5d381c93752a504e8de1fa10c9d51649853c70
ARG PYRTL_VERSION=0.12
ARG NUMPY_VERSION=2.3.3

FROM golang:${GO_VERSION}-bookworm@${GO_IMAGE_DIGEST} AS build
WORKDIR /src
COPY src/shared ./shared
COPY src/checkpoint-flusher ./checkpoint-flusher
COPY src/slurm-compute-node ./slurm-compute-node
WORKDIR /src/slurm-compute-node
RUN GOWORK=off go mod download && CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /gres-init ./cmd/gres-init && \
    CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /hardware-verifier ./cmd/hardware-verifier
WORKDIR /src/checkpoint-flusher
RUN GOWORK=off go mod download && CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /checkpoint-flusher .
RUN cc -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared -o /libaiorch_ring.so /src/shared/ringabi/abi.c

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
COPY --from=build /hardware-verifier /usr/local/bin/hardware-verifier
COPY --from=build /checkpoint-flusher /usr/local/bin/checkpoint-flusher
COPY --from=build /libaiorch_ring.so /usr/local/lib/libaiorch_ring.so
COPY src/shared/ringabi/abi.h /usr/local/include/aiorch_ring.h
COPY src/python-workloads/checkpointing /usr/lib/python3/dist-packages/checkpointing
COPY src/test/python-workloads/checkpointing/smoke.py /usr/lib/python3/dist-packages/checkpointing/smoke.py
COPY src/test/python-workloads/checkpointing/recovery_smoke.py /usr/lib/python3/dist-packages/checkpointing/recovery_smoke.py
COPY src/python-workloads/opentpu-harness/runtime.py /usr/local/bin/opentpu-runtime.py
COPY src/python-workloads/opentpu-harness/verify.py /usr/local/bin/opentpu-verify.py
COPY src/test/slurm-compute-node/cuda-smoke.py /usr/local/bin/cuda-smoke.py
ENV PYTHONPATH=/opt/opentpu:/opt/ai-orch \
    AIORCH_RING_LIBRARY=/usr/local/lib/libaiorch_ring.so \
    LD_LIBRARY_PATH=/usr/local/lib
