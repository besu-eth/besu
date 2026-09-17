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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

public class TracedJsonRpcProcessor implements JsonRpcProcessor {

  private final JsonRpcProcessor rpcProcessor;
  protected final LabelledMetric<Counter> rpcErrorsCounter;

  public TracedJsonRpcProcessor(
      final JsonRpcProcessor rpcProcessor, final MetricsSystem metricsSystem) {
    this.rpcProcessor = rpcProcessor;
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
    JsonRpcResponse jsonRpcResponse = rpcProcessor.process(id, method, metricSpan, request);
    if (RpcResponseType.ERROR == jsonRpcResponse.getType()) {
      JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) jsonRpcResponse;
      this.rpcErrorsCounter.labels(method.getName(), errorResponse.getErrorType().name()).inc();
      switch (errorResponse.getErrorType()) {
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
        case INVALID_RETURN_COMPLETE_TRANSACTION_PARAMS:
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
        case INVALID_TRANSACTION_ID_PARAMS:
        case INVALID_TRANSACTION_INDEX_PARAMS:
        case INVALID_TRANSACTION_LIMIT_PARAMS:
        case INVALID_TRANSACTION_TRACE_PARAMS:
        case INVALID_VERSIONED_HASH_PARAMS:
        case INVALID_VOTE_TYPE_PARAMS:
        case INVALID_WITHDRAWALS_PARAMS:
          metricSpan.setStatus(StatusCode.ERROR, "Invalid Params");
          break;
        case UNAUTHORIZED:
          metricSpan.setStatus(StatusCode.ERROR, "Unauthorized");
          break;
        case INTERNAL_ERROR:
          metricSpan.setStatus(StatusCode.ERROR, "Error processing JSON-RPC requestBody");
          break;
        default:
          metricSpan.setStatus(StatusCode.ERROR, "Unexpected error");
      }
    }
    metricSpan.end();
    return jsonRpcResponse;
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
    try {
      rpcProcessor.streamProcess(id, method, metricSpan, request, out, mapper);
    } catch (final IOException | RuntimeException e) {
      final String errorLabel =
          e instanceof InvalidJsonRpcRequestException ijrp
              ? ijrp.getRpcErrorType().name()
              : "INTERNAL_ERROR";
      rpcErrorsCounter.labels(method.getName(), errorLabel).inc();
      metricSpan.setStatus(StatusCode.ERROR, "Error processing JSON-RPC requestBody");
      throw e;
    } finally {
      metricSpan.end();
    }
  }
}
