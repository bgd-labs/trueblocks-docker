FROM golang:1.26.0-alpine3.23 AS builder

RUN apk --no-cache add \
    g++ gcc make cmake ninja git \
    libcurl curl curl-dev \
    python3 python3-dev \
    clang jq \
    linux-headers sqlite-dev bash sed

WORKDIR /root

ARG UPSTREAM_VER=v6.7.0

RUN git clone --depth 1 --no-single-branch --recurse-submodules --branch "${UPSTREAM_VER}" \
    https://github.com/TrueBlocks/trueblocks-core.git && \
    cd trueblocks-core && \
    mkdir -p build && \
    cd build && \
    ../scripts/go-work-sync.sh && \
    cmake ../src && \
    make -j "$(nproc)"

FROM alpine:3.23

RUN apk --no-cache add gzip libstdc++ libgcc libcurl python3 python3-dev procps bash curl nano findutils

COPY --from=builder /root/trueblocks-core/bin/chifra /usr/local/bin/chifra

COPY --from=builder /root/.local/share/trueblocks /root/.local/share/trueblocks
COPY chifra/trueBlocks.toml /root/.local/share/trueblocks/trueBlocks.toml

COPY chifra/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
