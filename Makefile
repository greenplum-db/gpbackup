all: build

ifndef GOPATH
$(error Environment variable GOPATH is not set)
endif

.DEFAULT_GOAL := all
BACKUP=gpbackup
RESTORE=gprestore
HELPER=gpbackup_helper
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')
GINKGO_FLAGS := -r --keep-going --randomize-suites --randomize-all --no-color
GIT_VERSION := $(shell git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
BACKUP_VERSION_STR=github.com/greenplum-db/gpbackup/backup.version=$(GIT_VERSION)
RESTORE_VERSION_STR=github.com/greenplum-db/gpbackup/restore.version=$(GIT_VERSION)
HELPER_VERSION_STR=github.com/greenplum-db/gpbackup/helper.version=$(GIT_VERSION)

# note that /testutils is not a production directory, but has unit tests to validate testing tools
SUBDIRS_HAS_UNIT=backup/ filepath/ history/ helper/ options/ report/ restore/ toc/ utils/ testutils/
SUBDIRS_ALL=$(SUBDIRS_HAS_UNIT) integration/ end_to_end/
GOLANG_LINTER=$(GOPATH)/bin/golangci-lint
GINKGO=$(GOPATH)/bin/ginkgo
GOIMPORTS=$(GOPATH)/bin/goimports
GO_BUILD=go build -mod=readonly
DEBUG=-gcflags=all="-N -l"

CUSTOM_BACKUP_DIR ?= "/tmp"
helper_path ?= $(BIN_DIR)/$(HELPER)

# Prefer gpsync as the newer utility, fall back to gpscp if not present (older installs)
ifeq (, $(shell which gpsync))
COPYUTIL=gpscp
else
COPYUTIL=gpsync
endif

depend :
	go mod download

$(GINKGO) :
	go install github.com/onsi/ginkgo/v2/ginkgo

$(GOIMPORTS) :
	go install golang.org/x/tools/cmd/goimports@latest

$(GOSQLITE) :
	go install github.com/mattn/go-sqlite3

format : $(GOIMPORTS)
		@goimports -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")

LINTER_VERSION=1.16.0
$(GOLANG_LINTER) :
		mkdir -p $(GOPATH)/bin
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v${LINTER_VERSION}

.PHONY : coverage integration end_to_end

lint : $(GOLANG_LINTER)
		golangci-lint run --tests=false

unit : $(GINKGO)
	ginkgo $(GINKGO_FLAGS) $(SUBDIRS_HAS_UNIT) 2>&1

unit_all_gpdb_versions : $(GINKGO)
		TEST_GPDB_VERSION=5.999.0 ginkgo $(GINKGO_FLAGS) $(SUBDIRS_HAS_UNIT) 2>&1
		TEST_GPDB_VERSION=6.999.0 ginkgo $(GINKGO_FLAGS) $(SUBDIRS_HAS_UNIT) 2>&1
		TEST_GPDB_VERSION=7.999.0 ginkgo $(GINKGO_FLAGS) $(SUBDIRS_HAS_UNIT) 2>&1 # GPDB main

integration : $(GINKGO)
	ginkgo $(GINKGO_FLAGS) integration 2>&1

test : build unit integration

end_to_end : $(GINKGO)
	ginkgo $(GINKGO_FLAGS) --timeout=3h --poll-progress-after=0s end_to_end -- --custom_backup_dir $(CUSTOM_BACKUP_DIR) 2>&1

coverage :
		@./show_coverage.sh

build : $(GOSQLITE)
		CGO_ENABLED=1 $(GO_BUILD) -tags '$(BACKUP)' -o $(BIN_DIR)/$(BACKUP) --ldflags '-X $(BACKUP_VERSION_STR)'
		CGO_ENABLED=1 $(GO_BUILD) -tags '$(RESTORE)' -o $(BIN_DIR)/$(RESTORE) --ldflags '-X $(RESTORE_VERSION_STR)'
		CGO_ENABLED=1 $(GO_BUILD) -tags '$(HELPER)' -o $(BIN_DIR)/$(HELPER) --ldflags '-X $(HELPER_VERSION_STR)'

debug :
		CGO_ENABLED=1 $(GO_BUILD) -tags '$(BACKUP)' -o $(BIN_DIR)/$(BACKUP) -ldflags "-X $(BACKUP_VERSION_STR)" $(DEBUG)
		CGO_ENABLED=1 $(GO_BUILD) -tags '$(RESTORE)' -o $(BIN_DIR)/$(RESTORE) -ldflags "-X $(RESTORE_VERSION_STR)" $(DEBUG)
		CGO_ENABLED=1 $(GO_BUILD) -tags '$(HELPER)' -o $(BIN_DIR)/$(HELPER) -ldflags "-X $(HELPER_VERSION_STR)" $(DEBUG)

build_linux :
		env GOOS=linux GOARCH=amd64 $(GO_BUILD) -tags '$(BACKUP)' -o $(BACKUP) -ldflags "-X $(BACKUP_VERSION_STR)"
		env GOOS=linux GOARCH=amd64 $(GO_BUILD) -tags '$(RESTORE)' -o $(RESTORE) -ldflags "-X $(RESTORE_VERSION_STR)"
		env GOOS=linux GOARCH=amd64 $(GO_BUILD) -tags '$(HELPER)' -o $(HELPER) -ldflags "-X $(HELPER_VERSION_STR)"

install :
		cp $(BIN_DIR)/$(BACKUP) $(BIN_DIR)/$(RESTORE) $(GPHOME)/bin
		@psql -X -t -d template1 -c 'select distinct hostname from gp_segment_configuration where content != -1' > /tmp/seg_hosts 2>/dev/null; \
		if [ $$? -eq 0 ]; then \
			$(COPYUTIL) -f /tmp/seg_hosts $(helper_path) =:$(GPHOME)/bin/$(HELPER); \
			if [ $$? -eq 0 ]; then \
				echo 'Successfully copied gpbackup_helper to $(GPHOME) on all segments'; \
			else \
				echo 'Failed to copy gpbackup_helper to $(GPHOME)'; \
				exit 1;	 \
			fi; \
		else \
			echo 'Database is not running, please start the database and run this make target again'; \
				exit 1;	 \
		fi; \
		rm /tmp/seg_hosts

clean :
		# Build artifacts
		rm -f $(BIN_DIR)/$(BACKUP) $(BACKUP) $(BIN_DIR)/$(RESTORE) $(RESTORE) $(BIN_DIR)/$(HELPER) $(HELPER)
		# Test artifacts
		rm -rf /tmp/go-build* /tmp/gexec_artifacts* /tmp/ginkgo*
		docker stop s3-minio # stop minio before removing its data directories
		docker rm s3-minio
		rm -rf /tmp/minio
		rm -f /tmp/minio_config.yaml
		# Code coverage files
		rm -rf /tmp/cover* /tmp/unit*
		go clean -i -r -x -testcache -modcache

error-report:
	@echo "Error messaging:"
	@echo ""
	@ag "gplog.Error|gplog.Fatal|ors.New|errors.Error|CheckClusterError|GpexpandFailureMessage =|errMsg :=" --ignore "*_test*" | grep -v "FatalOnError(err)" | grep -v ".Error()"

warning-report:
	@echo "Warning messaging:"
	@echo ""
	@ag "gplog.Warn" --ignore "*_test*"

info-report:
	@echo "Info and verbose messaging:"
	@echo ""
	@ag "gplog.Info|gplog.Verbose" --ignore "*_test*"

test-s3-local: build install
	${PWD}/plugins/generate_minio_config.sh
	mkdir -p /tmp/minio/gpbackup-s3-test
	docker run -d --name s3-minio --memory="2g" -p 9000:9000 -p 9001:9001 -v /tmp/minio:/data/minio quay.io/minio/minio server /data/minio --console-address ":9001"
	sleep 2 # Wait for minio server to start up
	${PWD}/plugins/plugin_test.sh $(BIN_DIR)/gpbackup_s3_plugin /tmp/minio_config.yaml
	docker stop s3-minio
	docker rm s3-minio
