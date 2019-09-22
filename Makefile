.PHONY: test
test:
	go test ./... -count 1 -v

proto:
	@go get github.com/golang/protobuf/protoc-gen-go
	@protoc -I . pubsub.proto --go_out=plugins=grpc,paths=source_relative:.
