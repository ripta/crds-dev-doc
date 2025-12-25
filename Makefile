# Set the shell to bash always
SHELL := /bin/bash

build:
	docker build -t crdsdev/doc:latest .

run-catchup:
	go run -v ./cmd/catchup

run-doc:
	go run -v ./cmd/doc

run-gitter:
	go run -v ./cmd/gitter

test:
	go test ./...

.PHONY: build-doc build-gitter
