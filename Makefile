.PHONY: test test-race lint run-cluster build

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	go vet ./...
	golangci-lint run

run-cluster:
	./scripts/run-cluster.sh
