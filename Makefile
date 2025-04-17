run:
	CGO_ENABLED=0 GOOS=darwin go build -o bin/darwin-cmd ./cmd/.
	./bin/darwin-cmd $(ARGS)

build: 
	go build -o bin/ss-cli ./cmd/.

build-all: 
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o bin/ss-cli-darwin-arm64 ./cmd/.
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o bin/ss-cli-darwin-amd64 ./cmd/.
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/ss-cli-linux-arm64 ./cmd/.
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/ss-cli-windows-amd64 ./cmd/.
