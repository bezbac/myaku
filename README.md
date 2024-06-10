## Profiling using samply

1. Install samply `cargo install --locked samply`
2. `cargo build --profile profiling --no-default-features`
3. `samply record ./target/profiling/myaku [ARGS]`
