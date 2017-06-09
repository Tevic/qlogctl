clean:
	go clean -i ./..

build:
		CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./bin/qlogctl_darwin_amd64 ./main.go
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/qlogctl_linux_amd64 ./main.go
