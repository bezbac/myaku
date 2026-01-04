## Profiling using samply

1. Install samply `cargo install --locked samply`
2. `cargo build --profile profiling --no-default-features`
3. `samply record ./target/profiling/myaku [ARGS]`

## Clippy linting

```
cargo clippy -- -D clippy::all -D clippy::pedantic -A clippy::redundant_closure -A clippy::redundant_closure_for_method_calls -A clippy::module_name_repetitions -A clippy::missing_errors_doc
```

## Git hooks

This repository manages git hooks through [pre-commit](https://pre-commit.com/).
To activate the git hooks, run `pre-commit install`

## Building the docker image

```
docker build -t myaku:latest .
```

## Running the docker image

```
docker run -i --rm myaku:latest query --url git@github.com:bezbac/myaku.git total-loc-over-time
```

Important!: The docker image does not have a valid SSH key setup. Only public repositories that can be cloned via http/https will work.
