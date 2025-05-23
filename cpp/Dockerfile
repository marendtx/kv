FROM ubuntu:22.04 AS base
RUN apt-get update && apt-get install -y \
    build-essential cmake git \
    libgtest-dev \
    && apt-get clean
RUN cd /usr/src/gtest && cmake . && make && cp lib/*.a /usr/lib
WORKDIR /app
COPY . .
RUN cmake -S . -B build && cmake --build build

FROM base AS test
CMD ["ctest", "--test-dir", "build", "--output-on-failure"]

FROM debian:bookworm-slim AS app
RUN apt-get update && apt-get install -y libstdc++6 && apt-get clean
WORKDIR /app
COPY --from=base /app/build/myapp .
CMD ["./myapp"]
