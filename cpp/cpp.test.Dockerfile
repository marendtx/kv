FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    build-essential cmake git \
    libgtest-dev \
    && apt-get clean

RUN cd /usr/src/gtest && cmake . && make && cp lib/*.a /usr/lib

WORKDIR /app
COPY . .

RUN cmake -S . -B build && cmake --build build

CMD ["ctest", "--test-dir", "build", "--output-on-failure"]
