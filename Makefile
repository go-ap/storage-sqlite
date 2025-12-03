SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GO ?= go
TEST := $(GO) test
TEST_FLAGS ?= -v -tags conformance
TEST_TARGET ?= .
GO111MODULE = on
PROJECT_NAME := $(shell basename $(PWD))

.PHONY: test coverage clean download

download: go.sum

go.sum:
	$(GO) mod tidy

test: go.sum clean
	CGO_ENABLED=1 $(TEST) $(TEST_FLAGS) -cover $(TEST_TARGET) -json > tests.json || true
	CGO_ENABLED=0 $(TEST) $(TEST_FLAGS) -cover $(TEST_TARGET) -json >> tests.json || true
	$(GO) run github.com/mfridman/tparse@latest -file tests.json

coverage: go.sum clean
	@mkdir ./_coverage
	CGO_ENABLED=1 $(TEST) $(TEST_FLAGS) -covermode=count -args -test.gocoverdir="$(PWD)/_coverage" $(TEST_TARGET) > /dev/null || true
	CGO_ENABLED=0 $(TEST) $(TEST_FLAGS) -covermode=count -args -test.gocoverdir="$(PWD)/_coverage" $(TEST_TARGET) > /dev/null || true
	$(GO) tool covdata percent -i=./_coverage/ -o $(PROJECT_NAME).coverprofile
	@$(RM) -r ./_coverage

clean:
	@$(RM) -r ./_coverage
	@$(RM) -v *.coverprofile
	@$(RM) -v tests.json

