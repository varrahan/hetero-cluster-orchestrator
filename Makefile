include versions.mk

GO_MODULES := \
	src/shared \
	src/slurm-operator \
	src/dra-driver \
	src/quantization-engine \
	src/watchdog-daemon \
	src/slurm-compute-node
BIN_DIR := $(CURDIR)/bin
GO_FILES := $(shell find src -type f -name '*.go' -print)
PYTHON_FILES := $(shell find src/python-workloads -type f -name '*.py' -print)
JSON_FILES := $(shell find docs -type f -name '*.json' -print)
SHELL_FILES := $(shell find test -type f -name '*.sh' -print 2>/dev/null)

.PHONY: all build test generate manifests verify fmt fmt-check vet versions \
	verify-versions manifests-check generated-check python-check json-check shell-check phase1-e2e

all: build

build: python-check
	@mkdir -p "$(BIN_DIR)"
	@cd src/shared && GOWORK=off go build ./...
	@cd src/slurm-operator && GOWORK=off go build -o "$(BIN_DIR)/slurm-operator" .
	@cd src/dra-driver && GOWORK=off go build -o "$(BIN_DIR)/dra-driver" .
	@cd src/quantization-engine && GOWORK=off go build -o "$(BIN_DIR)/quantization-engine" .
	@cd src/watchdog-daemon && GOWORK=off go build -o "$(BIN_DIR)/watchdog-daemon" .
	@cd src/slurm-compute-node && GOWORK=off go build -o "$(BIN_DIR)" ./cmd/...

test: python-check
	@assets="$$(cd src/slurm-operator && GOWORK=off go tool setup-envtest use -p path 1.35.0 --bin-dir "$(CURDIR)/.cache/envtest")"; \
	set -e; for module in $(GO_MODULES); do \
		printf 'testing %s\n' "$$module"; \
		if test "$$module" = src/slurm-operator; then \
			(cd "$$module" && KUBEBUILDER_ASSETS="$$assets" GOWORK=off go test ./...); \
		else \
			(cd "$$module" && GOWORK=off go test ./...); \
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
	@printf 'Go %s\nKubernetes %s\nSlurm %s\nOpenTPU %s\n' \
		'$(GO_VERSION)' '$(KUBERNETES_VERSION)' '$(SLURM_VERSION)' '$(OPENTPU_REVISION)'

verify: verify-versions generated-check fmt-check build test vet manifests-check json-check shell-check
