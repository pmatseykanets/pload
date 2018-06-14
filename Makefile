PLATFORM=$(shell go env GOOS)
ARCH=$(shell go env GOARCH)

default: build

build:
	go fmt ./...	
	@mkdir -p ./release
	@rm -rf ./release/*
	GOOS=darwin GOARCH=amd64 go build -o ./release/pload-darwin-amd64 ./pload.go
	GOOS=linux GOARCH=amd64 go build -o ./release/pload-linux-amd64 ./pload.go
	cp ./release/pload-$(PLATFORM)-$(ARCH) pload

install: build
	cp ./pload $(GOBIN)

.PHONY: build install
