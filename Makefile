run:
	CGO_ENABLED=0 GOOS=darwin go build -o bin/darwin-cmd ./cmd/.
	./bin/darwin-cmd $(ARGS)