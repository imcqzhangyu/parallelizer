BIN     := bin/parallelizer
VERSION := 0.1.0
LDFLAGS := -s -w -X parallelizer/internal/cli.Version=$(VERSION)

.PHONY: all build test race diff install clean release linux

all: build

build:
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN) ./cmd/parallelizer

test:
	go test ./...

race:
	go test -race ./...

# Compares exit status and output against GNU parallel. GNU_PARALLEL defaults
# to `parallel` found in PATH; override it to point at a source checkout:
#   GNU_PARALLEL="perl $HOME/projects/softw/parallel-20260922/src/parallel" make diff
diff: build
	GNU_PARALLEL="$${GNU_PARALLEL:-parallel}" ./test/diff_gnu.sh

install: build
	install -m 755 $(BIN) /usr/local/bin/parallelizer

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/parallelizer-linux-amd64 ./cmd/parallelizer

release: linux
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/parallelizer-darwin-arm64 ./cmd/parallelizer

clean:
	rm -rf bin
