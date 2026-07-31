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

# TODO(M6): launch a local 4-node cluster via cmd/node once p2p wiring lands.
run-cluster:
	@echo "run-cluster not wired yet — lands in M6 (p2p/network + cmd/node)"
