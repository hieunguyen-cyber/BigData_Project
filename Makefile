SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

KIND ?= kind
KUBECTL ?= kubectl
PYTHON ?= python3
KIND_CLUSTER ?= fraudstream-cluster
NAMESPACE ?= fraudstream

.PHONY: help check-tools cluster platform deploy bootstrap smoke persistence verify integration status test reproduce destroy batch-submit batch-verify

help:
	@echo "FraudStream W2 reproducibility commands"
	@echo "  make reproduce     Create/reuse Kind, deploy W2, verify and run integration smoke"
	@echo "  make persistence   Run Kafka, Cassandra and HDFS persistence checks"
	@echo "  make verify        Run full Bash infrastructure verifier"
	@echo "  make integration   Run simulator -> Kafka -> HDFS raw smoke"
	@echo "  make status        Show workload and PVC status"
	@echo "  make test          Run repository Python tests"
	@echo "  make destroy CONFIRM=1  Delete the whole Kind cluster and its local data"

check-tools:
	@command -v "$(KIND)" >/dev/null || { echo "ERROR: kind is required."; exit 1; }
	@command -v "$(KUBECTL)" >/dev/null || { echo "ERROR: kubectl is required."; exit 1; }
	@command -v "$(PYTHON)" >/dev/null || { echo "ERROR: python3 is required."; exit 1; }
	@docker info >/dev/null || { echo "ERROR: Docker daemon is not running."; exit 1; }
	@echo "PASS: Docker, kind, kubectl and Python are available."

cluster: check-tools
	@if $(KIND) get clusters | grep -Fxq "$(KIND_CLUSTER)"; then \
		echo "PASS: Kind cluster $(KIND_CLUSTER) already exists."; \
	else \
		$(KIND) create cluster --config infra/kind/kind-cluster.yaml --wait 5m; \
	fi
	@$(KUBECTL) cluster-info --context "kind-$(KIND_CLUSTER)" >/dev/null
	@$(KUBECTL) get nodes

platform: cluster
	@$(KUBECTL) apply -f infra/platform/00-namespace.yaml
	@$(KUBECTL) apply -f infra/platform/01-resource-quota.yaml
	@bash infra/scripts/check_preflight.sh

deploy: platform
	@$(KUBECTL) apply -f infra/cassandra/cassandra-config.yaml -f infra/cassandra/cassandra-service.yaml -f infra/cassandra/cassandra-statefulset.yaml
	@$(KUBECTL) apply -f infra/kafka/kafka.yaml
	@$(KUBECTL) apply -f infra/hdfs/hdfs-namenode.yaml -f infra/hdfs/hdfs-datanode.yaml
	@$(KUBECTL) rollout status statefulset/cassandra -n "$(NAMESPACE)" --timeout=300s
	@$(KUBECTL) rollout status statefulset/kafka -n "$(NAMESPACE)" --timeout=300s
	@$(KUBECTL) rollout status statefulset/hdfs-namenode -n "$(NAMESPACE)" --timeout=300s
	@$(KUBECTL) rollout status statefulset/hdfs-datanode -n "$(NAMESPACE)" --timeout=300s

bootstrap: deploy
	@bash infra/kafka/bootstrap_topics.sh
	@bash infra/hdfs/setup_datalake.sh
	@bash infra/cassandra/smoke_test.sh

smoke: bootstrap
	@bash infra/hdfs/smoke_test.sh
	@$(PYTHON) infra/kafka/smoke_test.py

persistence: bootstrap
	@bash infra/cassandra/smoke_test.sh --test-persistence
	@$(PYTHON) infra/kafka/smoke_test.py --test-persistence
	@bash infra/hdfs/smoke_test.sh --test-persistence

verify: bootstrap
	@bash infra/scripts/verify_infra.sh

integration: bootstrap
	@$(PYTHON) infra/scripts/integration_smoke.py

status:
	@$(KUBECTL) get pods,pvc -n "$(NAMESPACE)" -o wide

test:
	@$(PYTHON) -m pytest tests -q

BATCH_ID ?= batch_fixture_v1

batch-submit:
	@$(PYTHON) scripts/submit_batch.py --batch-id "$(BATCH_ID)"

batch-verify:
	@$(PYTHON) scripts/verify_batch.py --batch-id "$(BATCH_ID)"

reproduce: bootstrap verify integration
	@echo "PASS: W2 reproducibility flow completed. Run 'make persistence' for restart tests."

destroy:
	@if [[ "$(CONFIRM)" != "1" ]]; then \
		echo "Refusing destructive operation. Run: make destroy CONFIRM=1"; \
		exit 2; \
	fi
	@$(KIND) delete cluster --name "$(KIND_CLUSTER)"
