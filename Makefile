clean:
	go clean -i ./..

build: darwinx64 linuxx64

darwinx64:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./bin/qlogctl_darwin_amd64 ./main.go

linuxx64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/qlogctl_linux_amd64 ./main.go
