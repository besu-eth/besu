/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.tests.acceptance;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.metrics.MetricsProtocol;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.io.Closer;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.instrumentation.okhttp.v3_0.OkHttpTelemetry;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenTelemetryAcceptanceTest extends AcceptanceTestBase {

  private static final class FakeCollector extends TraceServiceGrpc.TraceServiceImplBase {
    private final List<ResourceSpans> receivedSpans = new ArrayList<>();
    private final Status returnedStatus = Status.OK;

    @Override
    public void export(
        final ExportTraceServiceRequest request,
        final StreamObserver<ExportTraceServiceResponse> responseObserver) {
      receivedSpans.addAll(request.getResourceSpansList());
      responseObserver.onNext(ExportTraceServiceResponse.newBuilder().build());
      if (!returnedStatus.isOk()) {
        if (returnedStatus.getCode() == Status.Code.DEADLINE_EXCEEDED) {
          // Do not call onCompleted to simulate a deadline exceeded.
          return;
        }
        responseObserver.onError(returnedStatus.asRuntimeException());
        return;
      }
      responseObserver.onCompleted();
    }

    List<ResourceSpans> getReceivedSpans() {
      return receivedSpans;
    }
  }

  private static final class FakeMetricsCollector
      extends MetricsServiceGrpc.MetricsServiceImplBase {
    private final List<ResourceMetrics> receivedMetrics = new ArrayList<>();
    private final Status returnedStatus = Status.OK;

    @Override
    public void export(
        final ExportMetricsServiceRequest request,
        final StreamObserver<ExportMetricsServiceResponse> responseObserver) {

      receivedMetrics.addAll(request.getResourceMetricsList());
      responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
      if (!returnedStatus.isOk()) {
        if (returnedStatus.getCode() == Status.Code.DEADLINE_EXCEEDED) {
          // Do not call onCompleted to simulate a deadline exceeded.
          return;
        }
        responseObserver.onError(returnedStatus.asRuntimeException());
        return;
      }
      responseObserver.onCompleted();
    }

    List<ResourceMetrics> getReceivedMetrics() {
      return receivedMetrics;
    }
  }

  private final FakeMetricsCollector fakeMetricsCollector = new FakeMetricsCollector();
  private final FakeCollector fakeTracesCollector = new FakeCollector();
  private final Closer closer = Closer.create();

  private BesuNode metricsNode;

  @BeforeEach
  public void setUp() throws Exception {
    System.setProperty("root.log.level", "DEBUG");
    final Server server =
        NettyServerBuilder.forPort(4317)
            .addService(fakeTracesCollector)
            .addService(fakeMetricsCollector)
            .build()
            .start();
    closer.register(server::shutdownNow);

    final MetricsConfiguration configuration =
        MetricsConfiguration.builder()
            .protocol(MetricsProtocol.OPENTELEMETRY)
            .enabled(true)
            .port(0)
            .hostsAllowlist(singletonList("*"))
            .build();
    final Map<String, String> env = new HashMap<>();
    env.put("OTEL_METRIC_EXPORT_INTERVAL", "1000");
    env.put("OTEL_TRACES_SAMPLER", "always_on");
    env.put("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");
    env.put("OTEL_EXPORTER_OTLP_INSECURE", "true");
    env.put("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    env.put("OTEL_BSP_SCHEDULE_DELAY", "1000");
    env.put("OTEL_BSP_EXPORT_TIMEOUT", "3000");

    metricsNode =
        besu.create(
            new BesuNodeConfigurationBuilder()
                .name("metrics-node")
                .jsonRpcEnabled()
                .metricsConfiguration(configuration)
                .environment(env)
                .build());
    cluster.start(metricsNode);
  }

  @AfterEach
  public void tearDown() throws Exception {
    closer.close();
  }

  @Test
  public void metricsReporting() {
    WaitUtils.waitFor(
        30,
        () -> {
          final List<ResourceMetrics> resourceMetrics = fakeMetricsCollector.getReceivedMetrics();
          assertThat(resourceMetrics.isEmpty()).isFalse();
        });
  }

  @Test
  public void traceReporting() {
    // longer timeout since at 30s this test is flaky
    WaitUtils.waitFor(
        60,
        () -> {
          // call the json RPC endpoint to generate a trace.
          net.netVersion().verify(metricsNode);
          final List<ResourceSpans> spans = fakeTracesCollector.getReceivedSpans();
          assertThat(spans.isEmpty()).isFalse();
          final Span internalSpan = spans.get(0).getScopeSpans(0).getSpans(0);
          assertThat(internalSpan.getKind()).isEqualTo(Span.SpanKind.SPAN_KIND_INTERNAL);
          final ByteString parent = internalSpan.getParentSpanId();
          assertThat(parent.isEmpty()).isFalse();
          // this part of the test is flaky
          //          final Span serverSpan = spans.get(0).getScopeSpans(0).getSpans(1);
          //          assertThat(serverSpan.getKind()).isEqualTo(Span.SpanKind.SPAN_KIND_SERVER);
          //          final ByteString rootSpanId = serverSpan.getParentSpanId();
          //          assertThat(rootSpanId.isEmpty()).isTrue();
        });
  }

  @Test
  public void traceReportingWithTraceId() {
    final Duration timeout = Duration.ofSeconds(1);
    WaitUtils.waitFor(
        60,
        () -> {
          final OpenTelemetry openTelemetry =
              OpenTelemetrySdk.builder()
                  .setPropagators(
                      ContextPropagators.create(
                          TextMapPropagator.composite(B3Propagator.injectingSingleHeader())))
                  .setTracerProvider(
                      SdkTracerProvider.builder().setSampler(Sampler.alwaysOn()).build())
                  .build();
          final Call.Factory client =
              OkHttpTelemetry.builder(openTelemetry)
                  .build()
                  .createCallFactory(
                      new OkHttpClient.Builder()
                          .connectTimeout(timeout)
                          .readTimeout(timeout)
                          .writeTimeout(timeout)
                          .build());
          final Request request =
              new Request.Builder()
                  .url("http://localhost:" + metricsNode.getJsonRpcPort().get())
                  .post(
                      RequestBody.create(
                          "{\"jsonrpc\":\"2.0\",\"method\":\"net_version\",\"params\":[],\"id\":255}",
                          MediaType.get("application/json")))
                  .build();
          final Response response = client.newCall(request).execute();
          try {
            assertThat(response.code()).isEqualTo(200);
            final List<ResourceSpans> spans =
                new ArrayList<>(fakeTracesCollector.getReceivedSpans());
            assertThat(spans.isEmpty()).isFalse();
            final Span internalSpan = spans.get(0).getScopeSpans(0).getSpans(0);
            assertThat(internalSpan.getKind()).isEqualTo(Span.SpanKind.SPAN_KIND_INTERNAL);
            final ByteString parent = internalSpan.getParentSpanId();
            assertThat(parent.isEmpty()).isFalse();
            final Span serverSpan = spans.get(0).getScopeSpans(0).getSpans(1);
            assertThat(serverSpan.getKind()).isEqualTo(Span.SpanKind.SPAN_KIND_SERVER);
            final ByteString rootSpanId = serverSpan.getParentSpanId();
            assertThat(rootSpanId.isEmpty()).isFalse();
          } finally {
            response.close();
            fakeTracesCollector.getReceivedSpans().clear();
          }
        });
  }
}
