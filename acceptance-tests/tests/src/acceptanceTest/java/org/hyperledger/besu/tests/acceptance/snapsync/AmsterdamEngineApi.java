/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.tests.acceptance.snapsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.eth.EthTransactions;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.web3j.protocol.core.methods.response.EthBlock;

/**
 * Drives a node over the Amsterdam Engine API (forkchoiceUpdatedV4, getPayloadV6, newPayloadV5) and
 * the {@code eth_getBlockAccessList} JSON-RPC method.
 */
class AmsterdamEngineApi {

  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String ZERO_HASH =
      "0x0000000000000000000000000000000000000000000000000000000000000000";
  // Devnet block gas limit, raised from 30M because of Amsterdam state-growth gas pricing.
  private static final String TARGET_GAS_LIMIT = "0x3b9aca00";

  // Block-build retry bounds: each attempt rebuilds with a longer window, so a loaded miner can
  // still pack every pooled transaction. A 1000-tx block takes ~8s on an unloaded machine.
  private static final int MAX_BUILD_ATTEMPTS = 10;
  private static final long MAX_BUILD_WINDOW_MILLIS = 16_000L;
  private static final long EMPTY_BUILD_WINDOW_MILLIS = 150L;
  private static final long INITIAL_BUILD_WINDOW_MILLIS = 4_000L;

  private final OkHttpClient httpClient = new OkHttpClient();
  private final ObjectMapper mapper = new ObjectMapper();
  private final EthTransactions ethTransactions;

  // Build window remembered across transaction-bearing blocks: once one heavy block proves a
  // longer window is needed, later blocks start from it instead of re-climbing from the minimum.
  private long txBuildWindowMillis = INITIAL_BUILD_WINDOW_MILLIS;

  AmsterdamEngineApi(final EthTransactions ethTransactions) {
    this.ethTransactions = ethTransactions;
  }

  record BuiltBlock(ObjectNode executionPayload, String executionRequests) {
    String blockHash() {
      return executionPayload.get("blockHash").asText();
    }
  }

  /** Builds a block containing all pooled transactions and imports it on the miner. */
  BuiltBlock buildBlock(
      final BesuNode miner,
      final String feeRecipient,
      final int expectedTxCount,
      final long slotNumber)
      throws IOException {
    final EthBlock.Block head = miner.execute(ethTransactions.block());
    final long baseTimestamp = head.getTimestamp().longValue() + 1;

    long windowMillis = expectedTxCount > 0 ? txBuildWindowMillis : EMPTY_BUILD_WINDOW_MILLIS;
    BuiltBlock built = null;
    for (int attempt = 0; attempt < MAX_BUILD_ATTEMPTS; attempt++) {
      // A distinct timestamp per attempt forces a fresh payload id, so each retry rebuilds over
      // the full transaction pool rather than reusing the finalized previous build.
      final String payloadId =
          startBuild(miner, head.getHash(), baseTimestamp + attempt, feeRecipient, slotNumber);
      sleep(windowMillis);
      final ObjectNode result = getPayload(miner, payloadId);
      final ObjectNode payload = (ObjectNode) result.get("executionPayload");
      if (payload.get("transactions").size() == expectedTxCount) {
        final JsonNode requests = result.get("executionRequests");
        built =
            new BuiltBlock(
                payload, requests != null && !requests.isNull() ? requests.toString() : "[]");
        break;
      }
      windowMillis = Math.min(windowMillis * 2, MAX_BUILD_WINDOW_MILLIS);
    }
    assertThat(built)
        .as(
            "miner did not build a block with %s transaction(s) within %s attempts",
            expectedTxCount, MAX_BUILD_ATTEMPTS)
        .isNotNull();
    if (expectedTxCount > 0) {
      txBuildWindowMillis = windowMillis;
    }

    importBlock(miner, built);
    return built;
  }

  /** newPayloadV5 (must return VALID) + forkchoiceUpdated, making the block canonical. */
  void importBlock(final BesuNode node, final BuiltBlock block) throws IOException {
    assertThat(newPayload(node, block))
        .as("engine_newPayloadV5 for block %s", block.blockHash())
        .isEqualTo("VALID");
    final JsonNode fcuResult = forkchoiceUpdated(node, block.blockHash(), block.blockHash());
    assertThat(fcuResult.get("payloadStatus").get("status").asText()).isEqualTo("VALID");
  }

  /** Submits a payload to a (syncing) node so it caches the header; any status is accepted. */
  void cachePayload(final BesuNode node, final BuiltBlock block) throws IOException {
    newPayload(node, block);
  }

  /** Submits a payload that must validate: the node has the world state to execute it. */
  void assertValidPayload(final BesuNode node, final BuiltBlock block) throws IOException {
    assertThat(newPayload(node, block)).isEqualTo("VALID");
  }

  /** forkchoiceUpdatedV4 with head only (no safe/finalized, no attributes): pure non-finality. */
  void setHead(final BesuNode node, final String headHash) throws IOException {
    forkchoiceUpdated(node, headHash, ZERO_HASH);
  }

  /** True once the node has the BAL for the given block (hex number) persisted locally. */
  boolean hasBlockAccessList(final BesuNode node, final String blockNumberHex) throws IOException {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockAccessList\",\"params\":[\""
            + blockNumberHex
            + "\"],\"id\":67}";
    try (Response response = jsonRpcCall(node, request).execute()) {
      if (response.code() != 200 || response.body() == null) {
        return false;
      }
      final JsonNode result = mapper.readTree(response.body().string()).get("result");
      return result != null && !result.isNull();
    }
  }

  /** forkchoiceUpdatedV4 with payload attributes; returns the payload id of the started build. */
  private String startBuild(
      final BesuNode miner,
      final String headHash,
      final long timestamp,
      final String feeRecipient,
      final long slotNumber)
      throws IOException {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV4\",\"params\":["
            + "{\"headBlockHash\":\""
            + headHash
            + "\",\"safeBlockHash\":\""
            + headHash
            + "\",\"finalizedBlockHash\":\""
            + ZERO_HASH
            + "\"},"
            + "{\"timestamp\":\"0x"
            + Long.toHexString(timestamp)
            + "\",\"prevRandao\":\""
            + ZERO_HASH
            + "\",\"suggestedFeeRecipient\":\""
            + feeRecipient
            + "\",\"withdrawals\":[],\"parentBeaconBlockRoot\":\""
            + ZERO_HASH
            + "\",\"slotNumber\":\"0x"
            + Long.toHexString(slotNumber)
            + "\",\"targetGasLimit\":\""
            + TARGET_GAS_LIMIT
            + "\"}],\"id\":67}";
    try (Response response = engineCall(miner, request).execute()) {
      assertThat(response.code()).isEqualTo(200);
      final String payloadId = result(response).get("payloadId").asText();
      assertThat(payloadId).isNotEmpty();
      return payloadId;
    }
  }

  /** engine_getPayloadV6 for the given payload id; returns the full result object. */
  private ObjectNode getPayload(final BesuNode miner, final String payloadId) throws IOException {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_getPayloadV6\",\"params\":[\""
            + payloadId
            + "\"],\"id\":67}";
    try (Response response = engineCall(miner, request).execute()) {
      assertThat(response.code()).isEqualTo(200);
      return (ObjectNode) result(response);
    }
  }

  /** engine_newPayloadV5; returns the payload status (VALID, SYNCING, ...). */
  private String newPayload(final BesuNode node, final BuiltBlock block) throws IOException {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_newPayloadV5\",\"params\":["
            + block.executionPayload()
            + ",[],\""
            + ZERO_HASH
            + "\","
            + block.executionRequests()
            + "],\"id\":67}";
    try (Response response = engineCall(node, request).execute()) {
      assertThat(response.code()).isEqualTo(200);
      return result(response).get("status").asText();
    }
  }

  private JsonNode forkchoiceUpdated(
      final BesuNode node, final String headHash, final String safeHash) throws IOException {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV4\",\"params\":["
            + "{\"headBlockHash\":\""
            + headHash
            + "\",\"safeBlockHash\":\""
            + safeHash
            + "\",\"finalizedBlockHash\":\""
            + ZERO_HASH
            + "\"},null],\"id\":67}";
    try (Response response = engineCall(node, request).execute()) {
      assertThat(response.code()).isEqualTo(200);
      return result(response);
    }
  }

  private JsonNode result(final Response response) throws IOException {
    return mapper.readTree(response.body().string()).get("result");
  }

  private Call engineCall(final BesuNode node, final String request) {
    return httpClient.newCall(
        new Request.Builder()
            .url(node.engineRpcUrl().get())
            .post(RequestBody.create(request, JSON))
            .build());
  }

  private Call jsonRpcCall(final BesuNode node, final String request) {
    return httpClient.newCall(
        new Request.Builder()
            .url(node.jsonRpcBaseUrl().get())
            .post(RequestBody.create(request, JSON))
            .build());
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
