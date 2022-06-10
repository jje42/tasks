version="$$(git describe --always --long --dirty)"
buildDate="$$(date +%Y-%m-%d)"

all:
	CGO_ENABLED=0 go build -ldflags="-X main.version=$(version) -X main.buildDate=$(buildDate)"