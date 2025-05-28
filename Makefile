compile:
	protoc -I api --cpp_out=api --grpc_out=api --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` api/*.proto

local-build:
	cmake -S . -B build -DCMAKE_PREFIX_PATH=$HOME/.local && cmake --build build

local-run:
	cmake -S . -B build -DCMAKE_PREFIX_PATH=$HOME/.local && cmake --build build && ./build/myapp && rm -rf build && rm -f tree_data/*.bin && rm -f *.log

local-test:
	cmake -S . -B build -DCMAKE_PREFIX_PATH=$HOME/.local && cmake --build build && ctest --test-dir build --output-on-failure && rm -rf build && rm -f tree_data/*.bin && rm -f *.log

local-clean:
	rm -rf build && rm -f tree_data/*.bin && rm -f *.log

docker-run:
	docker build --target app -t cpp-app -f Dockerfile . && docker run cpp-app

docker-test:
	docker build --target test -t cpp-test -f Dockerfile . && docker run cpp-test