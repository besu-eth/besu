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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinedJsonRpcProcessor implements JsonRpcProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(CombinedJsonRpcProcessor.class);

  private final LabelledMetric<OperationTimer> requestTimer;
  private final LabelledMetric<Counter> rpcErrorsCounter;

  public CombinedJsonRpcProcessor(final MetricsSystem metricsSystem) {
    this.requestTimer =
        metricsSystem.createLabelledTimer(
            BesuMetricCategory.RPC,
            "request_time",
            "Time to process JSON-RPC requests",
            "methodName");
    this.rpcErrorsCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "errors_count",
            "Number of errors per RPC method and RPC error type",
            "rpcMethod",
            "errorType");
  }

  @Override
  public JsonRpcResponse process(
      final JsonRpcRequestId id,
      final JsonRpcMethod method,
      final Span metricSpan,
      final JsonRpcRequestContext request) {
    try (final OperationTimer.TimingContext ignored =
        requestTimer.labels(request.getRequest().getMethod()).startTimer()) {
      JsonRpcResponse response = executeMethod(id, method, request);
      if (response.getType() == RpcResponseType.ERROR) {
        recordError(method, response, metricSpan);
      }
      metricSpan.end();
      return response;
    }
  }

  @Override
  @SuppressWarnings("UnusedVariable") // Parameters required by interface
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
      executeMethodAndStream(id, method, request, out, mapper);
      metricSpan.end();
    }
  }

  @SuppressWarnings("UnusedVariable") // Parameters required by interface
  private void executeMethodAndStream(
      final JsonRpcRequestId id,
      final JsonRpcMethod method,
      final JsonRpcRequestContext request,
      final OutputStream out,
      final ObjectMapper mapper)
      throws IOException {
    // Default implementation throws UnsupportedOperationException
    // Methods supporting streaming override this
    throw new UnsupportedOperationException(
        "Method " + method.getName() + " does not support streaming");
  }

  private JsonRpcResponse executeMethod(
      final JsonRpcRequestId id, final JsonRpcMethod method, final JsonRpcRequestContext request) {
    try {
      return method.response(request);
    } catch (final InvalidJsonRpcParameters e) {
      LOG.debug(
          "Invalid Params {} for method: {}, error: {}",
          Arrays.toString(request.getRequest().getParams()),
          method.getName(),
          e.getRpcErrorType().getMessage(),
          e);
      return new JsonRpcErrorResponse(id, e.getRpcErrorType());
    } catch (final RuntimeException e) {
      final JsonArray params = JsonObject.mapFrom(request.getRequest()).getJsonArray("params");
      LOG.error(String.format("Error processing method: %s %s", method.getName(), params), e);
      return new JsonRpcErrorResponse(id, RpcErrorType.INTERNAL_ERROR);
    }
  }

  private void recordError(
      final JsonRpcMethod method, final JsonRpcResponse response, final Span metricSpan) {
    JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    RpcErrorType errorType = errorResponse.getErrorType();
    rpcErrorsCounter.labels(method.getName(), errorType.name()).inc();
    setSpanStatus(metricSpan, errorType);
  }

  private void setSpanStatus(final Span span, final RpcErrorType errorType) {
    switch (errorType) {
      case INVALID_PARAMS:
      case INVALID_ACCOUNT_PARAMS:
      case INVALID_ADDRESS_HASH_PARAMS:
      case INVALID_ADDRESS_PARAMS:
      case INVALID_BLOB_COUNT:
      case INVALID_BLOB_GAS_USED_PARAMS:
      case INVALID_BLOCK_PARAMS:
      case INVALID_BLOCK_COUNT_PARAMS:
      case INVALID_BLOCK_HASH_PARAMS:
      case INVALID_BLOCK_INDEX_PARAMS:
      case INVALID_BLOCK_NUMBER_PARAMS:
      case INVALID_CALL_PARAMS:
      case INVALID_CONSOLIDATION_REQUEST_PARAMS:
      case INVALID_DATA_PARAMS:
      case INVALID_DEPOSIT_REQUEST_PARAMS:
      case INVALID_ENGINE_EXCHANGE_TRANSITION_CONFIGURATION_PARAMS:
      case INVALID_ENGINE_FORKCHOICE_UPDATED_PARAMS:
      case INVALID_ENGINE_FORKCHOICE_UPDATED_PAYLOAD_ATTRIBUTES:
      case INVALID_ENGINE_NEW_PAYLOAD_PARAMS:
      case INVALID_ENGINE_PREPARE_PAYLOAD_PARAMS:
      case INVALID_ENODE_PARAMS:
      case INVALID_EXCESS_BLOB_GAS_PARAMS:
      case INVALID_EXECUTION_REQUESTS_PARAMS:
      case INVALID_EXTRA_DATA_PARAMS:
      case INVALID_FILTER_PARAMS:
      case INVALID_HASH_RATE_PARAMS:
      case INVALID_ID_PARAMS:
      case INVALID_LOG_FILTER_PARAMS:
      case INVALID_LOG_LEVEL_PARAMS:
      case INVALID_MAX_RESULTS_PARAMS:
      case INVALID_METHOD_PARAMS:
      case INVALID_MIN_GAS_PRICE_PARAMS:
      case INVALID_MIN_PRIORITY_FEE_PARAMS:
      case INVALID_MIX_HASH_PARAMS:
      case INVALID_NONCE_PARAMS:
      case INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS:
      case INVALID_PARAM_COUNT:
      case INVALID_PAYLOAD_ID_PARAMS:
      case INVALID_PENDING_TRANSACTIONS_PARAMS:
      case INVALID_PLUGIN_NAME_PARAMS:
      case INVALID_POSITION_PARAMS:
      case INVALID_POW_HASH_PARAMS:
      case INVALID_PRIVATE_FROM_PARAMS:
      case INVALID_PRIVATE_FOR_PARAMS:
      case INVALID_PROPOSAL_PARAMS:
      case INVALID_REMOTE_CAPABILITIES_PARAMS:
      case INVALID_REWARD_PERCENTILES_PARAMS:
      case INVALID_REQUESTS_PARAMS:
      case INVALID_SEALER_ID_PARAMS:
      case INVALID_STORAGE_KEYS_PARAMS:
      case INVALID_SUBSCRIPTION_PARAMS:
      case INVALID_TARGET_GAS_LIMIT_PARAMS:
      case INVALID_TIMESTAMP_PARAMS:
      case INVALID_TRACE_CALL_MANY_PARAMS:
      case INVALID_TRACE_NUMBERS_PARAMS:
      case INVALID_TRACE_TYPE_PARAMS:
      case INVALID_TRANSACTION_PARAMS:
      case INVALID_TRANSACTION_HASH_PARAMS:
      case INVALID_TRANSACTION_INDEX_PARAMS:
      case INVALID_TRANSACTION_LIMIT_PARAMS:
      case INVALID_TRANSACTION_TRACE_PARAMS:
      case INVALID_VERSIONED_HASH_PARAMS:
      case INVALID_VOTE_TYPE_PARAMS:
      case INVALID_WITHDRAWALS_PARAMS:
        span.setStatus(StatusCode.ERROR, "Invalid Params");
        break;
      case UNAUTHORIZED:
        span.setStatus(StatusCode.ERROR, "Unauthorized");
        break;
      case INTERNAL_ERROR:
        span.setStatus(StatusCode.ERROR, "Error processing JSON-RPC requestBody");
        break;
      default:
        span.setStatus(StatusCode.ERROR, "Unexpected error");
    }
  }
}
