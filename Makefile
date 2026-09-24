.PHONY: build run run-mock test lint vet clean

BINARY      := flowgate
MOCK_BINARY := mockupstream
CMD         := ./cmd/flowgate
MOCK_CMD    := ./cmd/mockupstream

build:
	go build -o $(BINARY) $(CMD)
	go build -o $(MOCK_BINARY) $(MOCK_CMD)

run-mock:
	go run $(MOCK_CMD)

run: build
	UPSTREAM_URL=http://localhost:9000/events exec ./$(BINARY)

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test -race ./...

clean:
	rm -f $(BINARY) $(MOCK_BINARY)
