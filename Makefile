dist:
	@mkdir -p ./release
	@rm -rf ./release/*
	GOOS=darwin GOARCH=amd64 go build -o ./release/pload-darwin-amd64/pload ./pload.go
	GOOS=linux GOARCH=amd64 go build -o ./release/pload-linux-amd64/pload ./pload.go
