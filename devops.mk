# Define variables:
# 1. Force Make to use bash instead of the default standard sh
SHELL := /bin/bash
EXECUTABLE := das2go
KUBECTL := $(shell command -v kubectl 2>/dev/null)
ENV := $(if $(KUBECTL),$(shell $(KUBECTL) config get-contexts -o name 2>/dev/null))
CLUSTER := $(if $(KUBECTL),$(shell $(KUBECTL) config view --minify -o jsonpath='{.clusters[0].name}' 2>/dev/null))
MAKETIME := $(shell date +%Y%m%d-%H%M%S)
DAS2GO_SRC := $(shell pwd)

# Configuration variables:
TMP_DIR = $(DAS2GO_SRC)/tmp
CONFIG_REPO = https://github.com/dmwm/CMSKubernetes.git
CONFIG_BRANCH = master
CONFIG_DIR = $(TMP_DIR)/CMSKubernetes

# DAS service variables:
NAMESPACE = das
DAS_SERVERS = das-server
DEVOPS_TARGETS = devinit devpush devscale devrevert devstatus
DEVOPS_TARGET := $(firstword $(MAKECMDGOALS))
DAS_SERVER_WAS_SET := $(if $(filter undefined,$(origin DAS_SERVER)),,1)

# Accept the DAS service as a positional argument while preserving DAS_SERVER=...
ifeq (devscale,$(DEVOPS_TARGET))
  ifneq (,$(word 3,$(MAKECMDGOALS)))
    override DAS_SERVER := $(word 2,$(MAKECMDGOALS))
    DEV_REPLICAS := $(word 3,$(MAKECMDGOALS))
    DAS_SERVER_WAS_SET := 1
  else
    DEV_REPLICAS := $(word 2,$(MAKECMDGOALS))
  endif
else ifneq (,$(filter $(DEVOPS_TARGET),$(DEVOPS_TARGETS)))
  ifneq (,$(word 2,$(MAKECMDGOALS)))
    override DAS_SERVER := $(word 2,$(MAKECMDGOALS))
    DAS_SERVER_WAS_SET := 1
  endif
endif

DAS_SERVER ?= das-server
DAS_STATUS_SERVERS = $(if $(DAS_SERVER_WAS_SET),$(DAS_SERVER),$(DAS_SERVERS))
DAS_SERVER_DEV = $(DAS_SERVER)-dev
DAS_SERVER_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/services/$(DAS_SERVER).yaml
DAS_SERVER_DEV_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/services/$(DAS_SERVER_DEV).yaml
DAS_SERVER_HPA = $(DAS_SERVER)-hpa
DAS_HPA_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/hpa/das-hpa.yaml
DAS_ORIGINAL_REPLICAS_ANNOTATION = das2go.dev/original-replicas

# Positional arguments appear to Make as additional goals; define inert targets for them.
ifneq (,$(filter $(DEVOPS_TARGET),$(DEVOPS_TARGETS)))
  $(foreach arg,$(wordlist 2,99,$(MAKECMDGOALS)),$(eval $(arg):;@true))
endif

# External tools:
DASTOOLS_REPO = https://github.com/dmwm/DASTools.git
DASTOOLS_BRANCH = master
DASTOOLS_DIR = $(TMP_DIR)/DASTools
export PATH := $(DASTOOLS_DIR)/bin:$(PATH)

# DAS maps variables:
DASMAPS_PARSER = $(DASTOOLS_DIR)/bin/dasmaps_parser
DASMAPS_VALIDATOR = $(DASTOOLS_DIR)/bin/dasmaps_validator
DASMAPS_DIR = $(TMP_DIR)/dasmaps-dev.d/js
DASMAPS_BACKUP_DIR = $(TMP_DIR)/dasmaps-dev.d/backup
DASMAPS_DIR_REMOTE = /data/dasmaps-dev.d/js
DASMAPS_BACKUP_DIR_REMOTE = /data/dasmaps-dev.d/backup
DASMAPS_BACKUP_FILE = dasmaps_db.backup.$(ENV).$(MAKETIME).json
DASMAPS_BACKUP_LINK = $(DASMAPS_BACKUP_DIR)/latest
DASMAPS_BACKUP_FILE_LATEST = $(shell readlink -f $(DASMAPS_BACKUP_LINK))
DASMAPS_STAGE_DIR = $(TMP_DIR)/DASMaps/

# Local backup state:
BACKUP_DIR = $(TMP_DIR)/backup.d

# Setting up all needed ops directories.
_dummy := $(shell mkdir -p $(TMP_DIR) $(BACKUP_DIR) $(DASTOOLS_DIR) $(DASMAPS_DIR))

# Lazy assignment refreshes the Mongo pod name whenever a maps target uses it.
DAS_MONGO_POD = $(if $(KUBECTL),$(shell $(KUBECTL) -n $(NAMESPACE) get pod -l app=das-mongo -o jsonpath='{.items[0].metadata.name}' 2>/dev/null))

.PHONY: deploy clean build build_debug push_image run_deploy check_kubectl validate_dev_args \
	confirm_deploy setup_config setup_dastools devinit devpush devscale devrevert devstatus \
	mapsbackup mapspush mapsrevert run_dev_init run_dev_push run_dev_scale run_dev_redirect \
	run_dev_revert run_dev_status run_maps_fetch run_maps_generate run_maps_push \
	run_maps_cache_clean run_maps_backup run_maps_revert

check_kubectl:
	@[ -n "$(KUBECTL)" ] || { \
		echo "ERROR: kubectl was not found in PATH."; \
		exit 1; \
	}

validate_dev_args:
	@if [ "$(DEVOPS_TARGET)" = "devscale" ]; then \
		[ "$(words $(MAKECMDGOALS))" -eq 2 ] || [ "$(words $(MAKECMDGOALS))" -eq 3 ] || { \
			echo "ERROR: Usage: make -f devops.mk devscale [DAS_SERVER] <positive-replica-count>"; \
			exit 1; \
		}; \
		[[ "$(DEV_REPLICAS)" =~ ^[1-9][0-9]*$$ ]] || { \
			echo "ERROR: Usage: make -f devops.mk devscale [DAS_SERVER] <positive-replica-count>"; \
			exit 1; \
		}; \
	else \
		[ "$(words $(MAKECMDGOALS))" -le 2 ] || { \
			echo "ERROR: Usage: make -f devops.mk $(DEVOPS_TARGET) [DAS_SERVER]"; \
			exit 1; \
		}; \
	fi
	@[ "$(filter $(DAS_SERVER),$(DAS_SERVERS))" = "$(DAS_SERVER)" ] || { \
		echo "ERROR: Unsupported DAS service [ $(DAS_SERVER) ]."; \
		echo "Allowed services: $(DAS_SERVERS)"; \
		exit 1; \
	}

# Require interactive confirmation based on the detected environment.
confirm_deploy: check_kubectl
	@echo "========================================================================"
	@echo " WARNING: You are deploying at K8 environment: [ $(ENV) ]"
	@echo " Kubernetes cluster: [ $(CLUSTER) ]"
	@echo " DAS service: [ $(DAS_SERVER) ]"
	@echo "========================================================================"
	@if [ -z "$(ENV)" ]; then \
		echo "ERROR: Could not detect a pre-configured Kubernetes environment."; \
		exit 1; \
	fi
	@if [ "$$(printf '%s\n' "$(ENV)" | sed '/^$$/d' | wc -l)" -ne 1 ]; then \
		echo "ERROR: Expected exactly one configured Kubernetes context, found: [ $(ENV) ]"; \
		exit 1; \
	fi
	@{ [ "$(ENV)" = "cmsweb-testbed-backend" ] || \
		[[ "$(ENV)" =~ ^cmsweb-test[0-9]+[0-9]*$$ ]]; } || { \
		echo "ERROR: Environment [ $(ENV) ] is not allowed for this development workflow."; \
		exit 1; \
	}
	@printf "Are you sure you want to proceed? [y/N]: " && read ans < /dev/tty; \
	if [ "$$ans" != "y" ] && [ "$$ans" != "Y" ]; then \
		echo "Deployment aborted by user."; \
		exit 1; \
	fi

# Ensure tmp/ exists, then clone or update the configuration repository.
setup_config:
	@echo ">>> Preparing temporary config space..."
	@mkdir -p $(TMP_DIR)
	@if [ ! -d "$(CONFIG_DIR)/.git" ]; then \
		echo ">>> Cloning deployment repository and tracking branch [ $(CONFIG_BRANCH) ]..."; \
		git clone --branch $(CONFIG_BRANCH) $(CONFIG_REPO) $(CONFIG_DIR); \
	else \
		echo ">>> Repository exists. Fetching updates and switching to branch [ $(CONFIG_BRANCH) ]..."; \
		cd $(CONFIG_DIR) && \
		current_origin=$$(git remote get-url origin 2>/dev/null || true) && \
		if [ "$$current_origin" != "$(CONFIG_REPO)" ]; then \
			if [ -n "$$current_origin" ]; then \
				echo ">>> Updating configuration repository origin from [ $$current_origin ] to [ $(CONFIG_REPO) ]..."; \
				git remote set-url origin $(CONFIG_REPO); \
			else \
				echo ">>> Adding missing configuration repository origin [ $(CONFIG_REPO) ]..."; \
				git remote add origin $(CONFIG_REPO); \
			fi; \
		fi && \
		git fetch origin && \
		git checkout $(CONFIG_BRANCH) && \
		git pull origin $(CONFIG_BRANCH); \
	fi

setup_dastools:
	@echo ">>> Preparing temporary workspace for DASTools..."
	@mkdir -p $(TMP_DIR)
	@if [ ! -d "$(DASTOOLS_DIR)/.git" ]; then \
		echo ">>> Cloning deployment repository and tracking branch [ $(DASTOOLS_BRANCH) ]..."; \
		git clone --branch $(DASTOOLS_BRANCH) $(DASTOOLS_REPO) $(DASTOOLS_DIR); \
		cd $(DASTOOLS_DIR) && make; \
	else \
		echo ">>> Repository exists. Fetching updates and switching to branch [ $(DASTOOLS_BRANCH) ]..."; \
		cd $(DASTOOLS_DIR) && \
		git fetch origin && \
		git checkout $(DASTOOLS_BRANCH) && \
		git pull origin $(DASTOOLS_BRANCH); \
	fi

# Default DevOps flow.
deploy: confirm_deploy clean build push_image run_deploy

devinit: validate_dev_args confirm_deploy setup_config run_dev_init run_dev_redirect

devpush: validate_dev_args confirm_deploy build run_dev_push

devscale: validate_dev_args confirm_deploy run_dev_scale

devrevert: validate_dev_args confirm_deploy setup_config run_dev_revert

devstatus: validate_dev_args run_dev_status

mapsbackup: confirm_deploy run_maps_backup

mapspush: confirm_deploy setup_dastools \
	run_maps_fetch \
	run_maps_generate \
	run_maps_push \
	run_maps_cache_clean

mapsrevert: confirm_deploy run_maps_revert

# Keep direct invocation of low-level Kubernetes operations failure-sensitive too.
run_dev_init run_dev_push run_dev_scale run_dev_redirect run_dev_revert run_dev_status \
	run_maps_push run_maps_cache_clean run_maps_backup run_maps_revert: check_kubectl

# 1. Force a regular clean using the standard Makefile.
clean:
	$(MAKE) -f Makefile clean

# 2. Build the current source locally.
build:
	@echo ">>> Triggering regular build..."
	$(MAKE) -f Makefile build

build_debug:
	@echo ">>> Triggering debug build..."
	$(MAKE) -f Makefile build_debug

# 3. Package and push placeholder retained for future deployment development.
push_image:
	@echo ">>> TODO: Packaging and pushing image for $(ENV)..."

# 4. Deployment placeholder retained for future deployment development.
run_deploy:
	@echo ">>> TODO: Deploying $(EXECUTABLE) to $(ENV)..."

run_dev_init:
	@echo ">>> Deploying $(DAS_SERVER_DEV) to $(ENV)..."
	@test -f $(DAS_SERVER_DEV_MANIFEST) || { \
		echo "ERROR: Missing manifest $(DAS_SERVER_DEV_MANIFEST)"; \
		exit 1; \
	}
	@kubectl -n $(NAMESPACE) get deployment $(DAS_SERVER) && \
		kubectl -n $(NAMESPACE) get service das-mongo $(DAS_SERVER) && \
		kubectl -n $(NAMESPACE) get secret $(DAS_SERVER)-secrets \
			proxy-secrets robot-secrets hmac-secrets token-secrets

	# Follow the DBS controller split: constrain an HPA when present, otherwise scale the Deployment directly.
	@set -eu; \
	hpa_resource=$$(kubectl -n $(NAMESPACE) get hpa $(DAS_SERVER_HPA) --ignore-not-found -o name); \
	if [ -n "$$hpa_resource" ]; then \
		echo ">>> Constraining hpa/$(DAS_SERVER_HPA) to a single pod:"; \
		kubectl -n $(NAMESPACE) patch hpa $(DAS_SERVER_HPA) \
			-p '{"spec":{"minReplicas":1,"maxReplicas":1}}'; \
	else \
		original_replicas=$$(kubectl -n $(NAMESPACE) get deployment $(DAS_SERVER) \
			-o jsonpath='{.spec.replicas}'); \
		[[ "$$original_replicas" =~ ^[0-9]+$$ ]] || { \
			echo "ERROR: Invalid replica count [ $$original_replicas ] for deployment/$(DAS_SERVER)."; \
			exit 1; \
		}; \
		saved_replicas=$$(kubectl -n $(NAMESPACE) get deployment $(DAS_SERVER) \
			-o go-template='{{with index .metadata.annotations "$(DAS_ORIGINAL_REPLICAS_ANNOTATION)"}}{{.}}{{end}}'); \
		if [ -z "$$saved_replicas" ]; then \
			echo ">>> Preserving deployment/$(DAS_SERVER) replica count [ $$original_replicas ] in annotation $(DAS_ORIGINAL_REPLICAS_ANNOTATION)."; \
			kubectl -n $(NAMESPACE) annotate deployment $(DAS_SERVER) \
				$(DAS_ORIGINAL_REPLICAS_ANNOTATION)="$$original_replicas"; \
		else \
			[[ "$$saved_replicas" =~ ^[0-9]+$$ ]] || { \
				echo "ERROR: Invalid saved replica count [ $$saved_replicas ] in annotation $(DAS_ORIGINAL_REPLICAS_ANNOTATION)."; \
				exit 1; \
			}; \
			echo ">>> Preserving existing original replica count [ $$saved_replicas ]."; \
		fi; \
		echo ">>> Scaling deployment/$(DAS_SERVER) to a single pod:"; \
		kubectl -n $(NAMESPACE) scale deployment/$(DAS_SERVER) --replicas=1; \
	fi
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DAS_SERVER) --timeout=180s

	@echo ">>> Bringing up $(DAS_SERVER_DEV) development container..."
	@echo ">>> Checking deployment/$(DAS_SERVER_DEV)"
	@kubectl -n $(NAMESPACE) get deployment $(DAS_SERVER_DEV) >/dev/null 2>&1 && \
		echo ">>> OK: deployment/$(DAS_SERVER_DEV) exists" || \
		kubectl -n $(NAMESPACE) apply -f $(DAS_SERVER_DEV_MANIFEST)
	@echo ">>> Checking service/$(DAS_SERVER_DEV)"
	@kubectl -n $(NAMESPACE) get service $(DAS_SERVER_DEV) >/dev/null 2>&1 && \
		echo ">>> OK: service/$(DAS_SERVER_DEV) exists" || \
		kubectl -n $(NAMESPACE) apply -f $(DAS_SERVER_DEV_MANIFEST)
	@kubectl -n $(NAMESPACE) wait --for=jsonpath='{.status.phase}'=Running \
		pod -l app=$(DAS_SERVER_DEV) --timeout=180s
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DAS_SERVER_DEV) --timeout=180s
	@kubectl -n $(NAMESPACE) get deployment $(DAS_SERVER_DEV)
	@kubectl -n $(NAMESPACE) get service $(DAS_SERVER_DEV)
	@kubectl -n $(NAMESPACE) get pods -l app=$(DAS_SERVER_DEV) -o wide
	@echo ">>> Development pod initialized successfully."

run_dev_push:
	@echo ">>> Pushing locally built $(EXECUTABLE) payload to all $(DAS_SERVER_DEV) pods..."
	@set -eu; \
	pods=$$(kubectl -n $(NAMESPACE) get pods -l app=$(DAS_SERVER_DEV) \
		-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); \
	[ -n "$$pods" ] || { echo "ERROR: Development pods were not found. Run devinit first."; exit 1; }; \
	while IFS= read -r pod; do \
		echo ">>> Updating $$pod..."; \
		kubectl -n $(NAMESPACE) cp ./das2go "$$pod:/data/das2go" -c dev; \
		kubectl -n $(NAMESPACE) cp ./js "$$pod:/data/" -c dev; \
		kubectl -n $(NAMESPACE) cp ./css "$$pod:/data/" -c dev; \
		kubectl -n $(NAMESPACE) cp ./images "$$pod:/data/" -c dev; \
		kubectl -n $(NAMESPACE) cp ./templates "$$pod:/data/" -c dev; \
		kubectl -n $(NAMESPACE) cp ./examples "$$pod:/data/" -c dev; \
		kubectl -n $(NAMESPACE) exec "$$pod" -c dev -- chmod +x /data/das2go; \
		echo ">>> Restarting $(EXECUTABLE) at pod $$pod..."; \
		kubectl -n $(NAMESPACE) exec "$$pod" -c dev -- sh -c 'cd /data && \
			echo exec: $(EXECUTABLE) -config /etc/secrets/dasconfig.json && \
			if pkill -e $(EXECUTABLE); then :; else status=$$?; [ "$$status" -eq 1 ] || exit "$$status"; fi; \
			nohup /data/das2go -config /etc/secrets/dasconfig.json < /dev/null > /tmp/das2go-dev.log 2>&1 & \
			new_pid=$$!; \
			attempts=0; \
			while [ "$$attempts" -lt 10 ]; do \
				sleep 1; \
				if kill -0 "$$new_pid" 2>/dev/null && pgrep -x $(EXECUTABLE) | grep -qx "$$new_pid"; then exit 0; fi; \
				attempts=$$((attempts + 1)); \
			done; \
			echo "ERROR: $(EXECUTABLE) did not start." >&2; exit 1'; \
	done <<< "$$pods"

run_dev_scale:
	@[[ "$(DEV_REPLICAS)" =~ ^[1-9][0-9]*$$ ]] || { \
		echo "ERROR: Usage: make -f devops.mk devscale [DAS_SERVER] <positive-replica-count>"; \
		exit 1; \
	}
	@echo ">>> Scaling deployment/$(DAS_SERVER_DEV) to $(DEV_REPLICAS) pods..."
	@kubectl -n $(NAMESPACE) scale deployment/$(DAS_SERVER_DEV) --replicas=$(DEV_REPLICAS)
	@kubectl -n $(NAMESPACE) rollout status deployment/$(DAS_SERVER_DEV) --timeout=180s
	@kubectl -n $(NAMESPACE) get pods -l app=$(DAS_SERVER_DEV) -o wide

run_dev_redirect:
	@echo ">>> Preserving the current $(DAS_SERVER) Service manifest from $(ENV) to $(BACKUP_DIR):"
	@kubectl -n $(NAMESPACE) get service $(DAS_SERVER) -o yaml > \
		$(BACKUP_DIR)/$(DAS_SERVER).$(ENV).$(MAKETIME).yaml
	@echo ">>> Redirecting $(DAS_SERVER) traffic to $(DAS_SERVER_DEV) for $(ENV)..."
	@kubectl -n $(NAMESPACE) patch service $(DAS_SERVER) \
		-p '{"spec":{"selector":{"app":"$(DAS_SERVER_DEV)"}}}'

run_dev_revert:
	@set -eu; \
	saved_replicas=$$(kubectl -n $(NAMESPACE) get deployment $(DAS_SERVER) \
		-o go-template='{{with index .metadata.annotations "$(DAS_ORIGINAL_REPLICAS_ANNOTATION)"}}{{.}}{{end}}'); \
	if [ -n "$$saved_replicas" ]; then \
		[[ "$$saved_replicas" =~ ^[0-9]+$$ ]] || { \
			echo "ERROR: Invalid saved replica count [ $$saved_replicas ] in annotation $(DAS_ORIGINAL_REPLICAS_ANNOTATION)."; \
			exit 1; \
		}; \
		echo ">>> Restoring deployment/$(DAS_SERVER) to $$saved_replicas replica(s)..."; \
		kubectl -n $(NAMESPACE) scale deployment/$(DAS_SERVER) --replicas="$$saved_replicas"; \
		kubectl -n $(NAMESPACE) rollout status deployment/$(DAS_SERVER) --timeout=180s; \
		kubectl -n $(NAMESPACE) annotate deployment $(DAS_SERVER) \
			$(DAS_ORIGINAL_REPLICAS_ANNOTATION)-; \
	else \
		hpa_resource=$$(kubectl -n $(NAMESPACE) get hpa $(DAS_SERVER_HPA) --ignore-not-found -o name); \
		if [ -n "$$hpa_resource" ]; then \
			echo ">>> Restoring hpa/$(DAS_SERVER_HPA) from $(DAS_HPA_MANIFEST)..."; \
			limits=$$(awk -v target="$(DAS_SERVER_HPA)" ' \
				$$1 == "name:" && $$2 == target { selected=1 } \
				selected && $$1 == "minReplicas:" { min_replicas=$$2 } \
				selected && $$1 == "maxReplicas:" { max_replicas=$$2 } \
				selected && min_replicas != "" && max_replicas != "" { print min_replicas, max_replicas; exit } \
				' $(DAS_HPA_MANIFEST)); \
			read -r min_replicas max_replicas <<< "$$limits"; \
			[ -n "$$min_replicas" ] && [ -n "$$max_replicas" ] || { \
				echo "ERROR: Could not read $(DAS_SERVER_HPA) limits from $(DAS_HPA_MANIFEST)."; \
				exit 1; \
			}; \
			echo ">>> Restoring hpa/$(DAS_SERVER_HPA) replica limits to $$min_replicas/$$max_replicas..."; \
			kubectl -n $(NAMESPACE) patch hpa $(DAS_SERVER_HPA) \
				-p "{\"spec\":{\"minReplicas\":$$min_replicas,\"maxReplicas\":$$max_replicas}}"; \
		else \
			echo ">>> No saved direct-scaling state or hpa/$(DAS_SERVER_HPA); leaving deployment/$(DAS_SERVER) replicas unchanged."; \
		fi; \
	fi
	@echo ">>> Reverting $(DAS_SERVER) traffic for $(ENV):"
	@set -eu; \
	selector=$$(awk -v target="$(DAS_SERVER)" ' \
		$$1 == "name:" && $$2 == target { selected=1 } \
		selected && $$1 == "app:" { print $$2; exit } \
		' $(DAS_SERVER_MANIFEST)); \
	[ -n "$$selector" ] || { \
		echo "ERROR: Could not read the Service selector from $(DAS_SERVER_MANIFEST)."; \
		exit 1; \
	}; \
	kubectl -n $(NAMESPACE) patch service $(DAS_SERVER) \
		-p "{\"spec\":{\"selector\":{\"app\":\"$$selector\"}}}"

run_dev_status:
	@echo ">>> Environment [ $(ENV) ], cluster [ $(CLUSTER) ]"
	@printf '%-29s %-11s %-31s %-31s %-7s %s\n' \
		"SERVICE" "ROUTING" "SELECTOR" "ACTIVE DEPLOYMENT" "READY" "ENDPOINTS"; \
	for server in $(DAS_STATUS_SERVERS); do \
		dev_server="$$server-dev"; \
		selector=$$(kubectl -n $(NAMESPACE) get service "$$server" -o jsonpath='{.spec.selector.app}' 2>/dev/null || true); \
		case "$$selector" in \
			"$$dev_server") routing=REDIRECTED ;; \
			"$$server") routing=REGULAR ;; \
			"") routing=UNAVAILABLE ;; \
			*) routing=UNKNOWN ;; \
		esac; \
		desired=$$(kubectl -n $(NAMESPACE) get deployment "$$selector" -o jsonpath='{.spec.replicas}' 2>/dev/null || true); \
		ready=$$(kubectl -n $(NAMESPACE) get deployment "$$selector" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true); \
		if [ -n "$$desired" ]; then \
			ready=$${ready:-0}; ready_status="$$ready/$$desired"; \
		else \
			ready_status="-"; \
		fi; \
		active_deployment="$$selector"; \
		endpoint_ips=$$(kubectl -n $(NAMESPACE) get endpoints "$$server" \
			-o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' 2>/dev/null || true); \
		set -- $$endpoint_ips; endpoint_count=$$#; \
		printf '%-29s %-11s %-31s %-31s %-7s %s\n' \
			"$$server" "$$routing" "$$selector" "$$active_deployment" "$$ready_status" "$$endpoint_count"; \
	done

run_maps_fetch:
	@das_js_fetch https://raw.githubusercontent.com/dmwm/DASMaps/master/js $(DASMAPS_DIR)

run_maps_generate:
	@cd $(DASMAPS_DIR) && das_create_json_maps $(DAS2GO_SRC)/maps

run_maps_push:
	@echo ">>> Pushing locally generated DASMaps into das-mongo"
	@test -n "$(DAS_MONGO_POD)" || { \
		echo "ERROR: DAS_MONGO_POD is empty"; \
		exit 1; \
	}
	@test -d "$(DASMAPS_DIR)" || { \
		echo "ERROR: missing local DASMAPS_DIR=$(DASMAPS_DIR). Run run_maps_generate first."; \
		exit 1; \
	}
	@test "$$(find "$(DASMAPS_DIR)" -maxdepth 1 -name '*.js' | wc -l)" -gt 0 || { \
		echo "ERROR: no *.js maps found in DASMAPS_DIR=$(DASMAPS_DIR)"; \
		exit 1; \
	}
	@echo ">>> DAS_MONGO_POD=$(DAS_MONGO_POD)"
	@echo ">>> DASMAPS_DIR=$(DASMAPS_DIR)"
	@echo ">>> DASMAPS_DIR_REMOTE=$(DASMAPS_DIR_REMOTE)"
	@kubectl -n $(NAMESPACE) exec "$(DAS_MONGO_POD)" -- sh -lc 'rm -rf "$(DASMAPS_DIR_REMOTE)" && mkdir -p "$(DASMAPS_DIR_REMOTE)"'
	@tar -C "$(DASMAPS_DIR)" -cf - . | \
		kubectl -n $(NAMESPACE) exec -i "$(DAS_MONGO_POD)" -- sh -lc 'tar -C "$(DASMAPS_DIR_REMOTE)" -xf -'
	@kubectl -n $(NAMESPACE) exec "$(DAS_MONGO_POD)" -- sh -lc 'export PATH=/data:$$PATH; \
		dasmap=`cat /etc/secrets/dasmap`; echo dasmap: $$dasmap; \
		cp -f $(DASMAPS_DIR_REMOTE)/$$dasmap $(DASMAPS_DIR_REMOTE)/update_mapping_db.js'
	@kubectl -n $(NAMESPACE) exec "$(DAS_MONGO_POD)" -- sh -lc 'export PATH=/data:$$PATH; das_js_validate "$(DASMAPS_DIR_REMOTE)"'
	@kubectl -n $(NAMESPACE) exec "$(DAS_MONGO_POD)" -- sh -lc 'ls -1 "$(DASMAPS_DIR_REMOTE)"/*.js'
	@kubectl -n $(NAMESPACE) exec "$(DAS_MONGO_POD)" -- sh -lc 'export PATH=/data:$$PATH; das_js_import "$(DASMAPS_DIR_REMOTE)"'

run_maps_cache_clean:
	@echo ">>> Cleaning DAS query/result caches after map import..."
	@kubectl -n $(NAMESPACE) exec $(DAS_MONGO_POD) -- bash -lc 'export PATH=/data:$$PATH; \
		mongo --quiet --host localhost --port 8230 das --eval "db.cache.remove({}); db.merge.remove({})" \
	'

run_maps_backup:
	[[ -h $(DASMAPS_BACKUP_LINK) ]] && rm $(DASMAPS_BACKUP_LINK) || true
	@echo ">>> Creating backup of the current DAS maps at $(DASMAPS_BACKUP_DIR)" && \
	kubectl -n $(NAMESPACE) exec $(DAS_MONGO_POD) -- mkdir -p $(DASMAPS_BACKUP_DIR_REMOTE) && \
	kubectl -n $(NAMESPACE) exec $(DAS_MONGO_POD) -- sh -lc 'export PATH=/data/:$$PATH; mongoexport --host localhost --port 8230 --db mapping --collection db --out $(DASMAPS_BACKUP_DIR_REMOTE)/$(DASMAPS_BACKUP_FILE)' && \
	kubectl -n $(NAMESPACE) cp $(DAS_MONGO_POD):$(DASMAPS_BACKUP_DIR_REMOTE)/$(DASMAPS_BACKUP_FILE) $(DASMAPS_BACKUP_DIR)/$(DASMAPS_BACKUP_FILE) && \
	ln -s $(DASMAPS_BACKUP_DIR)/$(DASMAPS_BACKUP_FILE) $(DASMAPS_BACKUP_LINK)

run_maps_revert:
	@echo ">>> Reverting DASMAPS from file: $(DASMAPS_BACKUP_LINK) -> $$(readlink $(DASMAPS_BACKUP_LINK)) -> $(DASMAPS_BACKUP_FILE_LATEST)" && \
	kubectl -n $(NAMESPACE) cp "$$(readlink $(DASMAPS_BACKUP_LINK))" $(DAS_MONGO_POD):$(DASMAPS_DIR_REMOTE)/update_mapping_db.js && \
	kubectl -n $(NAMESPACE) exec $(DAS_MONGO_POD) -- rm -f $(DASMAPS_DIR_REMOTE)/mapping-schema-stamp && \
	kubectl -n $(NAMESPACE) exec $(DAS_MONGO_POD) -- sh -lc 'export PATH=/data/:$$PATH; /data/das_js_import $(DASMAPS_DIR_REMOTE)'
