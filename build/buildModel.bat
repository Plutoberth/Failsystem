REM Uncomment the next line to download the protobuf Go compiler. Note that $GOPATH/bin must be in PATH.
REM go get -u github.com/golang/protobuf/protoc-gen-go
protoc --go_out=plugins=grpc:./ model/model.proto
