# hcbridge build helpers. `make release` refuses a dirty tree: a deployed
# binary must be rebuildable from a commit (`hcbridge --version` shows
# "-dirty" otherwise).

BIN := hcbridge

.PHONY: build test check e2e release clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o $(BIN) .

test:
	go vet ./...
	go test -race ./...

check: test
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# End-to-end test against a throwaway Mosquitto container and the fake hob.
e2e:
	docker run --rm -d --name hcb-mqtt-e2e -p 127.0.0.1:18831:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
	HCBRIDGE_E2E_BROKER=127.0.0.1:18831 go test -tags e2e -race -count=1 -run E2E ./... ; status=$$?; docker stop hcb-mqtt-e2e >/dev/null; exit $$status

release: test
	@test -z "$$(git status --porcelain)" || { echo "refusing: working tree is dirty"; exit 1; }
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o $(BIN) .
	@./$(BIN) --version
	@sha256sum $(BIN)

clean:
	rm -f $(BIN)
