# Tracing

Hyperledger Besu integrates with the [open-telemetry](https://opentelemetry.io/) project to integrate tracing reporting.

This allows reporting all JSON-RPC traffic as traces.

To try out this example, start the Open Telemetry Collector and the Zipkin service with:

`$> docker-compose up`

Start besu with:

`$> OTEL_RESOURCE_ATTRIBUTES="service.name=besu-dev" OTEL_EXPORTER_OTLP_INSECURE=true ./gradlew run --args="--network=ephemery --rpc-http-enabled --metrics-enabled --metrics-protocol=opentelemetry"`

Try interacting with the JSON-RPC API. Here is a simple example using cURL:

`$> curl -X POST --data '{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":53}' http://localhost:8545`

Open the Zipkin UI by browsing to http://localhost:9411/

You will be able to see the detail of your traces.

References:
* [OpenTelemetry Environment Variable Specification](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/configuration/sdk-environment-variables.md)
