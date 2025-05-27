FROM ubuntu:24.04 AS base

# install C++ and GoogleTest
RUN apt-get update && apt-get install -y \
    build-essential cmake git \
    libgtest-dev autoconf libtool pkg-config openssl libssl-dev libz-dev \
    && apt-get clean

RUN cd /usr/src/gtest && cmake . && make && cp lib/*.a /usr/lib

# gRPC
RUN git clone --recurse-submodules -b v1.72.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc $HOME/grpc && \
    mkdir -p $HOME/.local && \
    cd $HOME/grpc && \
    mkdir -p cmake/build && \
    cd cmake/build && \
    cmake ../.. \
      -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_CXX_STANDARD=17 \
      -DCMAKE_INSTALL_PREFIX=$HOME/.local && \
    make -j 4 && \
    make install

# NuRaft
RUN git clone https://github.com/eBay/NuRaft.git $HOME/NuRaft && \
    cd $HOME/NuRaft && \
    git submodule update --init && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=$HOME/.local && \
    make && \
    make install

WORKDIR /app
COPY . .
RUN cmake -S . -B build -DCMAKE_PREFIX_PATH=/root/.local && cmake --build build

FROM ubuntu:24.04 AS test
RUN apt-get update && apt-get install -y libstdc++6 && apt-get clean
WORKDIR /app
COPY --from=base /app/build/runTests .

COPY --from=base /root/.local/lib/*.so* /usr/local/lib/

ENV LD_LIBRARY_PATH=/usr/local/lib

CMD ["./runTests"]

FROM ubuntu:24.04 AS app
RUN apt-get update && apt-get install -y libstdc++6 && apt-get clean
WORKDIR /app
COPY --from=base /app/build/myapp .

COPY --from=base /root/.local/lib/*.so* /usr/local/lib/

ENV LD_LIBRARY_PATH=/usr/local/lib

CMD ["./myapp"]
