FROM rustlang/rust:nightly-slim

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --bin main

ENTRYPOINT ["./target/release/main"]
