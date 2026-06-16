# Define variables:
# 1. Force Make to use bash instead of the default standard sh
SHELL := /bin/bash
EXECUTABLE := das2go
ENV := $(shell kubectl config get-contexts -o name)
MAKETIME := $(shell date +%Y%m%d-%H%M%S)
DAS2GO_SRC := $(shell pwd)

# Configuration variables:
TMP_DIR     := $(DAS2GO_SRC)/tmp
# CONFIG_REPO = git@github.com:dmwm/CMSKubernetes.git
# CONFIG_REPO = https://github.com/dmwm/CMSKubernetes.git
# CONFIG_BRANCH = master
CONFIG_REPO = https://github.com/todor-ivanov/CMSKubernetes.git
CONFIG_BRANCH = feature_CreateDasDevEnv
CONFIG_DIR  = $(TMP_DIR)/CMSKuberenetes
DAS_SERVER_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/services/das-server.yaml
DAS_SERVER_DEV_MANIFEST = $(CONFIG_DIR)/kubernetes/cmsweb/services/das-server-dev.yaml

# External tools:
DASTOOLS_REPO = https://github.com/dmwm/DASTools.git
DASTOOLS_BRANCH = master
DASTOOLS_DIR  = $(TMP_DIR)/DASTools
export PATH := $(DASTOOLS_DIR)/bin:$(PATH)

# DAS maps variables:
DASMAPS_PARSER = $(DASTOOLS_DIR)/bin/dasmaps_parser
DASMAPS_VALIDATOR = $(DASTOOLS_DIR)/bin/dasmaps_validator
DASMAPS_DIR =          $(TMP_DIR)/dasmaps-dev.d/js
DASMAPS_BACKUP_DIR =   $(TMP_DIR)/dasmaps-dev.d/backup
DASMAPS_DIR_REMOTE =        /data/dasmaps-dev.d/js
DASMAPS_BACKUP_DIR_REMOTE = /data/dasmaps-dev.d/backup
DASMAPS_BACKUP_FILE = dasmaps_db.backup.$(ENV).$(MAKETIME).json
DASMAPS_BACKUP_LINK = $(DASMAPS_BACKUP_DIR)/latest
DASMAPS_BACKUP_FILE_LATEST = $(shell readlink -f $(DASMAPS_BACKUP_LINK))
DASMAPS_STAGE_DIR = $(TMP_DIR)/DASMaps/js


# Backup variables
BACKUP_DIR = $(TMP_DIR)/backup.d/


# Setting up all needed ops directories
_dummy := $(shell mkdir -p $(TMP_DIR) $(BACKUP_DIR) $(DASTOOLS_DIR) $(DASMAPS_DIR))

# Using Lazy assignment for refreshing the pod names anytime we call them from bellow e.g. as $(POD):
DAS_SERVER_DEV_POD = $(shell kubectl -n das get pod -l app=das-server-dev -o jsonpath='{.items[0].metadata.name}')
DAS_SERVER_POD = $(shell kubectl -n das get pod -l app=das-server -o jsonpath='{.items[0].metadata.name}')
DAS_MONGO_POD = $(shell kubectl -n das get pod -l app=das-mongo -o jsonpath='{.items[0].metadata.name}')




.PHONY: deploy build confirm_deploy devpush devinit run_dev_push run_dev_init \
	run_dev_redirect run_maps_push run_maps_backup run_maps_revert run_maps_generate \
	run_maps_cache_clean run_maps_transform run_maps_fetch setup_config setup_dastools

# Confirmation step: Require user interactive confirmation based on the detected environment
confirm_deploy:
	@echo "========================================================================"
	@echo " WARNING: You are deploying at K8 environment: [ $(ENV) ]"
	@echo "========================================================================"
	@if [ -z "$(ENV)" ]; then \
		echo "ERROR: Could not detect an active Kubernetes context."; \
		exit 1; \
	fi
	@printf "Are you sure you want to proceed? [y/N]: " && read ans < /dev/tty; \
	if [ "$$ans" != "y" ] && [ "$$ans" != "Y" ]; then \
		echo "Deployment aborted by user."; \
		exit 1; \
	fi

# ConfigSetup step: Ensure tmp/ exists, clone or update the configuration repo
setup_config:
	@echo ">>> Preparing temporary config space..."
	@mkdir -p $(TMP_DIR)
	@if [ ! -d "$(CONFIG_DIR)/.git" ]; then \
		echo ">>> Cloning deployment repository and tracking branch [ $(CONFIG_BRANCH) ]..."; \
		git clone --branch $(CONFIG_BRANCH) $(CONFIG_REPO) $(CONFIG_DIR); \
	else \
		echo ">>> Repository exists. Fetching updates and switching to branch [ $(CONFIG_BRANCH) ]..."; \
		cd $(CONFIG_DIR) && \
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
		@cd $(DASTOOLS_DIR) && make ; \
	else \
		echo ">>> Repository exists. Fetching updates and switching to branch [ $(DASTOOLS_BRANCH) ]..."; \
		cd $(DASTOOLS_DIR) && \
		git fetch origin && \
		git checkout $(DASTOOLS_BRANCH) && \
		git pull origin $(DASTOOLS_BRANCH); \
	fi


# Default DevOps flow
deploy: confirm_deploy clean build push_image run_deploy

devinit: confirm_deploy setup_config run_dev_init run_dev_redirect

devpush: confirm_deploy build run_dev_push

devrevert: confirm_deploy run_dev_revert

mapsbackup: confirm_deploy run_maps_backup

mapspush: confirm_deploy setup_dastools \
	run_maps_generate \
	run_maps_push \
	run_maps_cache_clean

mapsrevert: confirm_deploy run_maps_revert


# 1. Force a production build using the standard Makefile
clean:
	$(MAKE) -f Makefile clean

build:
	@echo ">>> Triggering regular build..."
	$(MAKE) -f Makefile build

build_debug:
	@echo ">>> Triggering regular build..."
	$(MAKE) -f Makefile build_debug

# 2. Package and push (Example: Docker)
push_image:
	@echo ">>> TODO: Packaging and pushing image for $(ENV)..."
	# docker build -t myrepo/$(APP_NAME):latest .
	# docker push myrepo/$(APP_NAME):latest

# 3. Deploy to infrastructure (Example: Kubernetes or AWS)
run_deploy:
	@echo ">>> TODO: Deploying $(APP_NAME) to $(ENV)..."
	# kubectl apply -f k8s/deployment.yaml

run_dev_init:
	@echo ">>> Deploying $(APP_NAME) to $(ENV)..."  && \
		kubectl -n das get service das-mongo && \
		kubectl -n das get service das-server && \
	 	kubectl -n das get secret das-server-secrets proxy-secrets robot-secrets hmac-secrets token-secrets

	# For facilitating debugging we must scale down the currently running service to a single instance
	@echo ">>> Scaling down the current deployment to a single pod:"
	@kubectl -n das scale deployment/das-server --replicas=1
	@kubectl -n das rollout status deployment/das-server

	@echo ">>> Bringing up das-server-dev empty container"

	@echo ">>> Checking deployment/das-server-dev"
	@kubectl -n das get deployment das-server-dev >/dev/null 2>&1 && \
		echo ">>> OK: deployment/das-server-dev exists" || \
		kubectl -n das apply -f $(DAS_SERVER_DEV_MANIFEST)

	@echo ">>> Checking service/das-server-dev"
	@kubectl -n das get service das-server-dev >/dev/null 2>&1 && \
		echo ">>> OK: service/das-server-dev exists" || \
		kubectl -n das apply -f $(DAS_SERVER_DEV_MANIFEST)

	@kubectl -n das rollout status deployment/das-server-dev
	@kubectl -n das get deployment das-server-dev
	@kubectl -n das get service das-server-dev
	@kubectl -n das get pods -l app=das-server-dev -o wide

	@echo ">>> Deployment completed successfully."

run_dev_push:
	@echo ">>> Pushing locally built executable for $(EXECUTABLE) to pod $(DAS_SERVER_DEV_POD)..."
	@kubectl -n das cp ./das2go  $(DAS_SERVER_DEV_POD):/data/das2go -c dev

	# @kubectl -n das cp ./js $(DAS_SERVER_DEV_POD):/data/js -c dev
	# @kubectl -n das cp ./css $(DAS_SERVER_DEV_POD):/data/css -c dev
	# @kubectl -n das cp ./images $(DAS_SERVER_DEV_POD):/data/images -c dev
	# @kubectl -n das cp ./templates $(DAS_SERVER_DEV_POD):/data/templates -c dev
	# @kubectl -n das cp ./examples $(DAS_SERVER_DEV_POD):/data/examples -c dev
	# @kubectl -n das exec $(DAS_SERVER_DEV_POD) -c dev -- chmod +x /data/das2go

	@kubectl -n das cp ./js        $(DAS_SERVER_DEV_POD):/data/ -c dev
	@kubectl -n das cp ./css       $(DAS_SERVER_DEV_POD):/data/ -c dev
	@kubectl -n das cp ./images    $(DAS_SERVER_DEV_POD):/data/ -c dev
	@kubectl -n das cp ./templates $(DAS_SERVER_DEV_POD):/data/ -c dev
	@kubectl -n das cp ./examples  $(DAS_SERVER_DEV_POD):/data/ -c dev
	@kubectl -n das exec $(DAS_SERVER_DEV_POD) -c dev -- chmod +x /data/das2go

	@echo ">>> Restarting $(EXECUTABLE) at pod $(DAS_SERVER_DEV_POD)..."
	@kubectl -n das exec $(DAS_SERVER_DEV_POD) -c dev -- sh -c "cd /data/ && \
		echo exec: $(EXECUTABLE) -config /etc/secrets/dasconfig.json && \
		pkill -e $(EXECUTABLE) && \
		exec /data/das2go -config /etc/secrets/dasconfig.json < /dev/null > /dev/null 2>&1 &"

run_dev_redirect:
	@echo ">>> Preserving the current das-server service manifest from $(ENV) to $(BACKUP_DIR):"
	@kubectl -n das get service das-server -o yaml > $(BACKUP_DIR)/das-server.$(ENV).$(MAKETIME).yaml

	@echo ">>> Redirecting ingress traffic to $(DAS_SERVER_DEV_POD) for $(ENV):"
	@kubectl -n das patch service das-server -p '{"spec":{"selector":{"app":"das-server-dev"}}}'

run_dev_revert:
	@echo ">>> Reverting ingress traffic to $(DAS_SERVER_POD) for $(ENV):"
	@kubectl -n das patch service das-server -p '{"spec":{"selector":{"app":"das-server"}}}'

run_maps_fetch:
	@rm -rf   $(DASMAPS_STAGE_DIR)
	@mkdir -p $(DASMAPS_STAGE_DIR)
	@das_js_fetch https://raw.githubusercontent.com/dmwm/DASMaps/master/js $(DASMAPS_STAGE_DIR)

# run_maps_generate:
# 	@echo ">>> Generating DAS maps from local source tree..."
# 	@mkdir -p $(DASMAPS_DIR)
# 	@rm -f $(DASMAPS_GENERATED_FILE)
# 	@for map in $(DASMAPS_YAML_ALL); do \
# 	  echo ">>> parsing $$map"; \
# 	  $(DASMAPS_PARSER) -input $$map >> $(DASMAPS_GENERATED_FILE); \
# 	done
# 	@echo ">>> validating $(DASMAPS_GENERATED_FILE)"
# 	@$(DASMAPS_VALIDATOR) -dasmaps $(DASMAPS_GENERATED_FILE)

run_maps_generate:
	@rm -rf $(DASMAPS_DIR)
	@mkdir -p $(DASMAPS_DIR)
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
	@kubectl -n das exec "$(DAS_MONGO_POD)" -- sh -lc 'rm -rf "$(DASMAPS_DIR_REMOTE)" && mkdir -p "$(DASMAPS_DIR_REMOTE)"'
	@tar -C "$(DASMAPS_DIR)" -cf - . | \
	  kubectl -n das exec -i "$(DAS_MONGO_POD)" -- sh -lc 'tar -C "$(DASMAPS_DIR_REMOTE)" -xf -'
	@kubectl -n das exec "$(DAS_MONGO_POD)" -- sh -lc 'export PATH=/data:$$PATH; \
		dasmap=`cat /etc/secrets/dasmap`; echo dasmap: $$dasmap; \
		cp -f $(DASMAPS_DIR_REMOTE)/$$dasmap $(DASMAPS_DIR_REMOTE)/update_mapping_db.js'
	@kubectl -n das exec "$(DAS_MONGO_POD)" -- sh -lc 'export PATH=/data:$$PATH; das_js_validate "$(DASMAPS_DIR_REMOTE)"'
	@kubectl -n das exec "$(DAS_MONGO_POD)" -- sh -lc 'ls -1 "$(DASMAPS_DIR_REMOTE)"/*.js'
	@kubectl -n das exec "$(DAS_MONGO_POD)" -- sh -lc 'export PATH=/data:$$PATH; das_js_import "$(DASMAPS_DIR_REMOTE)"'


run_maps_cache_clean:
	@echo ">>> Cleaning DAS query/result caches after map import..."
	@kubectl -n das exec $(DAS_MONGO_POD) -- bash -lc 'export PATH=/data:$$PATH; \
	  mongo --quiet --host localhost --port 8230 das --eval "db.cache.remove({}); db.merge.remove({})" \
	'

run_maps_backup:
	[[ -h $(DASMAPS_BACKUP_LINK) ]] && rm $(DASMAPS_BACKUP_LINK) || true
	@echo ">>> Creating backup of the current DAS maps at $(DASMAPS_BACKUP_DIR)" && \
	kubectl -n das exec $(DAS_MONGO_POD) -- mkdir -p $(DASMAPS_BACKUP_DIR_REMOTE) && \
	kubectl -n das exec $(DAS_MONGO_POD) -- sh -lc 'export PATH=/data/:$$PATH; mongoexport --host localhost --port 8230 --db mapping --collection db --out $(DASMAPS_BACKUP_DIR_REMOTE)/$(DASMAPS_BACKUP_FILE)' && \
	kubectl -n das cp $(DAS_MONGO_POD):$(DASMAPS_BACKUP_DIR_REMOTE)/$(DASMAPS_BACKUP_FILE) $(DASMAPS_BACKUP_DIR)/$(DASMAPS_BACKUP_FILE) && \
	ln -s $(DASMAPS_BACKUP_DIR)/$(DASMAPS_BACKUP_FILE) $(DASMAPS_BACKUP_LINK)

run_maps_revert:
	@echo ">>> Rverting DASMAPS from file:  $(DASMAPS_BACKUP_LINK) -> $$(readlink $(DASMAPS_BACKUP_LINK)) -> $(DASMAPS_BACKUP_FILE_LATEST)" && \
	kubectl -n das cp "$$(readlink $(DASMAPS_BACKUP_LINK))" $(DAS_MONGO_POD):$(DASMAPS_DIR_REMOTE)/update_mapping_db.js && \
	kubectl -n das exec $(DAS_MONGO_POD) -- rm -f $(DASMAPS_DIR_REMOTE)/mapping-schema-stamp && \
	kubectl -n das exec $(DAS_MONGO_POD) -- sh -lc 'export PATH=/data/:$$PATH; /data/das_js_import $(DASMAPS_DIR_REMOTE)'
