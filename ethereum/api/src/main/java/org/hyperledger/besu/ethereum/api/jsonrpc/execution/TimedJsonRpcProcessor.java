/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.execution;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;

public class TimedJsonRpcProcessor implements JsonRpcProcessor {

  private final JsonRpcProcessor rpcProcessor;
  private final LabelledMetric<OperationTimer> requestTimer;

  public TimedJsonRpcProcessor(
      final JsonRpcProcessor rpcProcessor, final LabelledMetric<OperationTimer> requestTimer) {
    this.rpcProcessor = rpcProcessor;
    this.requestTimer = requestTimer;
  }

  @Override
  public JsonRpcResponse process(
      final JsonRpcRequestId id,
      final JsonRpcMethod method,
      final Span metricSpan,
      final JsonRpcRequestContext request) {
    try (final OperationTimer.TimingContext ignored =
        requestTimer.labels(request.getRequest().getMethod()).startTimer()) {
      return rpcProcessor.process(id, method, metricSpan, request);
    }
  }

  @Override
  public void streamProcess(
      final JsonRpcRequestId id,
      final JsonRpcMethod method,
      final Span metricSpan,
      final JsonRpcRequestContext request,
      final OutputStream out,
      final ObjectMapper mapper)
      throws IOException {
    try (final OperationTimer.TimingContext ignored =
        requestTimer.labels(request.getRequest().getMethod()).startTimer()) {
      rpcProcessor.streamProcess(id, method, metricSpan, request, out, mapper);
    }
  }
}
