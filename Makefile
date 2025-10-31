.PHONY: proto
proto:
	# go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest
	cd proto && \
	protoc \
		--go_out=. \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
    	--go-grpc_out=. \
    	--go-grpc_opt paths=source_relative \
    	--plugin protoc-gen-go-grpc="${GOBIN}/protoc-gen-go-grpc" \
      	--go-vtproto_out=. \
      	--go-vtproto_opt paths=source_relative \
      	--plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
      	--go-vtproto_opt=features=marshal+unmarshal+unmarshal_unsafe+size+pool+equal+clone \
	    *.proto

license-check:
	@command -v go-license > /dev/null || go install github.com/palantir/go-license@latest
	find . -type f -name '*.go' | grep -v '.pb.go' | xargs go-license --config=.github/license.yml --verify

license-format:
	@command -v go-license > /dev/null || go install github.com/palantir/go-license@latest
	find . -type f -name '*.go' | grep -v '.pb.go' | xargs go-license --config=.github/license.yml
