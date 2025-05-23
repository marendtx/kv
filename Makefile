all: go-test-local go-test-container go-run-local go-run-container

cpp-test-local:
	cd cpp && cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure && rm -rf build

cpp-test-container:
	cd cpp && docker build --target test -t cpp-test -f Dockerfile . && docker run cpp-test

cpp-run-container:
	cd cpp && docker build --target app -t cpp-app -f Dockerfile . && docker run cpp-app

go-test-local:
	cd go && go test -v ./...

go-test-container:
	cd go && docker build -f go.test.Dockerfile -t go-test . && docker run go-test

go-run-local:
	cd go && go run main.go

go-run-container:
	cd go && docker build -f go.app.Dockerfile -t go-app . && docker run go-app