clean:
	go clean -i ./..

build:
	go build -o ./bin/logctl ./main.go
