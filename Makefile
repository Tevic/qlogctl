clean:
	go clean -i ./..

build:
	go build -o ./bin/logctl ./src/logctl.go
