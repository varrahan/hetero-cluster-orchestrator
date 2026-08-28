include versions.mk

GO_MODULES := \
	src/shared \
	src/checkpoint-flusher \
	src/quantization-engine \
	src/optical-dra-driver \
	src/slurm-operator \
	src/dra-driver \
	src/slurm-compute-node \
	src/watchdog-daemon
BIN_DIR := $(CURDIR)/bin
GO_FILES := $(shell find src -type f -name '*.go' -print)
PYTHON_FILES := $(shell find src -type f -name '*.py' -print)
JSON_FILES := $(shell find docs -type f -name '*.json' -print)
SHELL_FILES := $(shell find test -type f -name '*.sh' -print 2>/dev/null)

.PHONY: all build test generate manifests verify fmt fmt-check vet versions \
	verify-versions manifests-check generated-check python-check python-test ring-lib json-check shell-check \
	phase1-e2e phase2-e2e phase2-gpu-e2e phase3-e2e phase4-e2e \
	phase5-e2e optical-demo demo examples-check release-manifests

all: build

build: python-check ring-lib
	@mkdir -p "$(BIN_DIR)"
	@cd src/slurm-operator && GOWORK=off go build -o "$(BIN_DIR)/slurm-operator" .
	@cd src/dra-driver && GOWORK=off go build -o "$(BIN_DIR)/dra-driver" .
	@cd src/optical-dra-driver && GOWORK=off go build -o "$(BIN_DIR)/optical-dra-driver" .
	@cd src/slurm-compute-node && GOWORK=off go build -o "$(BIN_DIR)" ./cmd/...
	@cd src/checkpoint-flusher && GOWORK=off go build -o "$(BIN_DIR)/checkpoint-flusher" .
	@cd src/quantization-engine && GOWORK=off go build -o "$(BIN_DIR)/quantization-engine" .
	@cd src/watchdog-daemon && GOWORK=off go build -o "$(BIN_DIR)/watchdog-daemon" .

test: python-check python-test
	@staged="$$(mktemp -d)"; trap 'rm -rf "$$staged"' EXIT; \
	cp -R src "$$staged/"; cp -R src/test/. "$$staged/src/"; \
	assets="$$(cd src/slurm-operator && GOWORK=off go tool setup-envtest use -p path 1.35.0 --bin-dir "$(CURDIR)/.cache/envtest")"; \
	set -e; for module in $(GO_MODULES); do \
		printf 'testing %s\n' "$$module"; \
		if test "$$module" = src/slurm-operator; then \
			(cd "$$staged/$$module" && KUBEBUILDER_ASSETS="$$assets" GOWORK=off go test ./...); \
		else \
			(cd "$$staged/$$module" && GOWORK=off go test ./...); \
		fi; \
	done

generate:
	@mkdir -p src/manifests/crds src/manifests/rbac
	@cd src/slurm-operator && GOWORK=off go tool controller-gen object paths=./api/...
	@cd src/slurm-operator && GOWORK=off go tool controller-gen \
		crd paths=./api/... output:crd:artifacts:config=../manifests/crds
	@cd src/slurm-operator && GOWORK=off go tool controller-gen \
		rbac:roleName=slurm-operator paths=./... output:rbac:artifacts:config=../manifests/rbac

manifests:
	kubectl kustomize src/manifests

phase1-e2e:
	./test/phase1/e2e.sh

phase2-e2e:
	./test/phase2/e2e.sh

phase2-gpu-e2e:
	./test/phase2/gpu-e2e.sh

optical-demo:
	./test/optical/e2e.sh

demo:
	@KIND_CLUSTER="$${KIND_CLUSTER:-demo}" KEEP_KIND=1 $(MAKE) phase4-e2e
	@printf 'Demo cluster retained. Inspect: rtk kubectl -n slurm-system get pods,resourceclaims\n'
	@printf 'Remove it: rtk kind delete cluster --name %s\n' "$${KIND_CLUSTER:-demo}"

phase3-e2e:
	./test/phase3/e2e.sh

phase4-e2e:
	./test/phase4/e2e.sh

phase5-e2e:
	./test/phase5/e2e.sh

examples-check:
	./test/phase5/examples.py

release-manifests:
	./test/phase5/release-manifests.sh

ring-lib:
	@mkdir -p "$(BIN_DIR)"
	@$(CC) -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared \
		-o "$(BIN_DIR)/libaiorch_ring.so" src/shared/ringabi/abi.c

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@test -z "$$(gofmt -l $(GO_FILES))" || { \
		printf 'Go files need formatting:\n%s\n' "$$(gofmt -l $(GO_FILES))"; \
		exit 1; \
	}

vet:
	@set -e; for module in $(GO_MODULES); do \
		printf 'vetting %s\n' "$$module"; \
		(cd "$$module" && GOWORK=off go vet ./...); \
	done

python-check:
	@python3 -c 'import ast, pathlib; [ast.parse(pathlib.Path(path).read_bytes(), filename=path) for path in "$(PYTHON_FILES)".split()]'

python-test: ring-lib
	@AIORCH_RING_LIBRARY="$(BIN_DIR)/libaiorch_ring.so" PYTHONPATH=src/python-workloads \
		python3 -m unittest discover -s src/test/python-workloads/checkpointing -p 'test_*.py'

json-check:
	@set -e; for file in $(JSON_FILES); do python3 -m json.tool "$$file" >/dev/null; done

shell-check:
	@test -z "$(SHELL_FILES)" || bash -n $(SHELL_FILES)

manifests-check:
	@kubectl kustomize src/manifests >/dev/null

generated-check:
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
		cp -R src/slurm-operator/api/v1alpha1 src/manifests/crds src/manifests/rbac "$$tmp"; \
		$(MAKE) generate >/dev/null; \
		diff -ru "$$tmp/v1alpha1" src/slurm-operator/api/v1alpha1; \
		diff -ru "$$tmp/crds" src/manifests/crds; \
		diff -ru "$$tmp/rbac" src/manifests/rbac

verify-versions:
	@actual="$$(go env GOVERSION)"; test "$$actual" = "go$(GO_VERSION)" || { \
		printf 'expected Go %s, got %s\n' "$(GO_VERSION)" "$$actual"; \
		exit 1; \
	}
	@set -e; for file in go.work $(addsuffix /go.mod,$(GO_MODULES)); do \
		grep -qx 'toolchain go$(GO_VERSION)' "$$file" || { \
			printf '%s does not pin Go %s\n' "$$file" "$(GO_VERSION)"; \
			exit 1; \
		}; \
	done

versions:
	@printf 'Go %s\nKubernetes %s\nSlurm %s\nNode Problem Detector %s\nOpenTPU %s\nNVIDIA toolkit %s\nPyRTL %s\nNumPy %s\n' \
		'$(GO_VERSION)' '$(KUBERNETES_VERSION)' '$(SLURM_VERSION)' '$(NODE_PROBLEM_DETECTOR_VERSION)' '$(OPENTPU_REVISION)' \
		'$(NVIDIA_TOOLKIT_VERSION)' '$(PYRTL_VERSION)' '$(NUMPY_VERSION)'

verify: verify-versions generated-check fmt-check build test vet manifests-check examples-check json-check shell-check
