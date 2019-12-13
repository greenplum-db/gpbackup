all: build

ifndef GOPATH
$(error Environment variable GOPATH is not set)
endif

.DEFAULT_GOAL := all
BACKUP=gpbackup
RESTORE=gprestore
HELPER=gpbackup_helper
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')
GINKGO_FLAGS := -r -randomizeSuites -randomizeAllSpecs -noisySkippings=false

GIT_VERSION := $(shell git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
BACKUP_VERSION_STR="-X github.com/greenplum-db/gpbackup/backup.version=$(GIT_VERSION)"
RESTORE_VERSION_STR="-X github.com/greenplum-db/gpbackup/restore.version=$(GIT_VERSION)"
HELPER_VERSION_STR="-X github.com/greenplum-db/gpbackup/helper.version=$(GIT_VERSION)"

# note that /testutils is not a production directory, but has unit tests to validate testing tools
SUBDIRS_HAS_UNIT=backup/ backup_filepath/ backup_history/ helper/ options/ restore/ utils/ testutils/
SUBDIRS_ALL=$(SUBDIRS_HAS_UNIT) integration/ end_to_end/
GOLANG_LINTER=$(GOPATH)/bin/golangci-lint
GINKGO=$(GOPATH)/bin/ginkgo
GOIMPORTS=$(GOPATH)/bin/goimports

CUSTOM_BACKUP_DIR ?= "/tmp"
helper_path ?= $(BIN_DIR)/$(HELPER)

$(DEP) :
		mkdir -p $(GOPATH)/bin
		curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

depend :
		go mod download

$(GINKGO) :
		go get github.com/onsi/ginkgo/ginkgo

$(GOIMPORTS) : depend
		@cd vendor/golang.org/x/tools/cmd/goimports; go install .

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
		ginkgo -r -keepGoing -randomizeSuites -noisySkippings=false -randomizeAllSpecs $(SUBDIRS_HAS_UNIT) 2>&1

integration : $(GINKGO)
		ginkgo -r -randomizeSuites -noisySkippings=false -randomizeAllSpecs integration 2>&1

test : unit integration

end_to_end : $(GINKGO)
		ginkgo -r -randomizeSuites -slowSpecThreshold=10 -noisySkippings=false -randomizeAllSpecs end_to_end -- --custom_backup_dir $(CUSTOM_BACKUP_DIR) 2>&1

coverage :
		@./show_coverage.sh

build :
		go build -tags '$(BACKUP)' $(GOFLAGS) -o $(BIN_DIR)/$(BACKUP) -ldflags $(BACKUP_VERSION_STR)
		go build -tags '$(RESTORE)' $(GOFLAGS) -o $(BIN_DIR)/$(RESTORE) -ldflags $(RESTORE_VERSION_STR)
		go build -tags '$(HELPER)' $(GOFLAGS) -o $(BIN_DIR)/$(HELPER) -ldflags $(HELPER_VERSION_STR)

build_linux :
		env GOOS=linux GOARCH=amd64 go build -tags '$(BACKUP)' $(GOFLAGS) -o $(BACKUP) -ldflags $(BACKUP_VERSION_STR)
		env GOOS=linux GOARCH=amd64 go build -tags '$(RESTORE)' $(GOFLAGS) -o $(RESTORE) -ldflags $(RESTORE_VERSION_STR)
		env GOOS=linux GOARCH=amd64 go build -tags '$(HELPER)' $(GOFLAGS) -o $(HELPER) -ldflags $(HELPER_VERSION_STR)

install :
		@psql -t -d template1 -c 'select distinct hostname from gp_segment_configuration where content != -1' > /tmp/seg_hosts 2>/dev/null; \
		if [ $$? -eq 0 ]; then \
			gpscp -f /tmp/seg_hosts $(helper_path) =:$(GPHOME)/bin/$(HELPER); \
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
		# Code coverage files
		rm -rf /tmp/cover* /tmp/unit*

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
