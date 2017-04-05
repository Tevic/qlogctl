clean:
	go clean -i ./...

build:
	go build -o ./bin/qbana ./src/view/main.go
