# hcbridge build helpers. `make release` refuses a dirty tree: a deployed
# binary must be rebuildable from a commit (`hcbridge --version` shows
# "-dirty" otherwise).

BIN := hcbridge

.PHONY: build test check release clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o $(BIN) .

test:
	go vet ./...
	go test -race ./...

check: test
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

release: test
	@test -z "$$(git status --porcelain)" || { echo "refusing: working tree is dirty"; exit 1; }
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o $(BIN) .
	@./$(BIN) --version
	@sha256sum $(BIN)

clean:
	rm -f $(BIN)
