test-local: cpp-test-local go-test-local

test-container: cpp-test-container go-test-container

cpp-proto-compile:
	cd cpp && protoc -I api --cpp_out=api --grpc_out=api --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` api/hello.proto

cpp-build:
	cd cpp && cmake -S . -B build -DCMAKE_PREFIX_PATH=$HOME/.local && cmake --build build

cpp-test-local:
	cd cpp && cmake -S . -B build -DCMAKE_PREFIX_PATH=$HOME/.local && cmake --build build && ctest --test-dir build --output-on-failure && rm -rf build && rm -f tree_data/*.bin

cpp-test-container:
	cd cpp && docker build --target test -t cpp-test -f Dockerfile . && docker run cpp-test

cpp-run-local:
	cd cpp && cmake -S . -B build -DCMAKE_PREFIX_PATH=$HOME/.local && cmake --build build && ./build/myapp && rm -rf build && rm -f tree_data/*.bin

cpp-run-container:
	cd cpp && docker build --target app -t cpp-app -f Dockerfile . && docker run cpp-app

go-test-local:
	cd go && go test -v ./...

go-test-container:
	cd go && docker build -f Dockerfile.test -t go-test . && docker run go-test

go-run-local:
	cd go && go run main.go

go-run-container:
	cd go && docker build -f Dockerfile.app -t go-app . && docker run go-app