## Opentelemetry

The `--opentelemetry` flag prints opentelemetry spans to stdout in jsonlines format.
To analyze them using a tool like jaeger these first need to be combined into a single json file.
`jq -c --slurp '{resourceSpans: map(.resourceSpans[])}' < trace.jsonlines > trace.json`

To open jaeger run: `docker run -d -p4317:4317 -p16686:16686 jaegertracing/all-in-one:latest`
