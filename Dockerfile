FROM rust:1.96-slim-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY syntax ./syntax
COPY storage ./storage
COPY index ./index
COPY mvcc ./mvcc
COPY engine ./engine
COPY server ./server
COPY bench ./bench
RUN cargo build --release -p quantadb-server -p quantadb-bench

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/quantadb-server /usr/local/bin/
COPY --from=builder /build/target/release/loadgen /usr/local/bin/
ENV QUANTA_DATA_DIR=/var/lib/quantadb \
    QUANTA_LISTEN_ADDRESS=0.0.0.0:54321 \
    QUANTA_PG_LISTEN_ADDRESS=0.0.0.0:55432
VOLUME /var/lib/quantadb
EXPOSE 54321 55432
ENTRYPOINT ["quantadb-server"]
