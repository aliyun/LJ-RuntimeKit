BINARY=bin/job-controller
CMD=cmd/controller

.PHONY: build clean docker

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY) ./$(CMD)

clean:
	rm -rf bin/

docker:
	docker build -t lj-runtimekit-job-controller:latest .
