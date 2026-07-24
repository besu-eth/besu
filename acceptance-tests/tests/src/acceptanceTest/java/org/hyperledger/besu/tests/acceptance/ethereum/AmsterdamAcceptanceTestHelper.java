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
package org.hyperledger.besu.tests.acceptance.ethereum;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.eth.EthTransactions;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.web3j.protocol.core.methods.response.EthBlock;

/** Acceptance test helper for Amsterdam fork. */
public class AmsterdamAcceptanceTestHelper {
  protected static final MediaType MEDIA_TYPE_JSON =
      MediaType.parse("application/json; charset=utf-8");

  private final OkHttpClient httpClient;
  private final ObjectMapper mapper;
  private final BesuNode besuNode;
  private final EthTransactions ethTransactions;
  private static final String PARENT_BEACON_BLOCK_ROOT_TEST =
      "0x0000000000000000000000000000000000000000000000000000000000000000";

  private long blockTimeStamp = 0;
  private long slotNumber = 0;

  AmsterdamAcceptanceTestHelper(final BesuNode besuNode, final EthTransactions ethTransactions) {
    this.besuNode = besuNode;
    this.ethTransactions = ethTransactions;
    httpClient = new OkHttpClient();
    mapper = new ObjectMapper();
  }

  public void buildNewBlock() throws IOException {
    final EthBlock.Block block = besuNode.execute(ethTransactions.block());

    blockTimeStamp += 1;
    slotNumber += 1;
    final Call buildBlockRequest =
        createEngineCall(createForkChoiceRequest(block.getHash(), blockTimeStamp, slotNumber));

    final String payloadId;
    try (final Response buildBlockResponse = buildBlockRequest.execute()) {
      payloadId =
          mapper
              .readTree(buildBlockResponse.body().string())
              .get("result")
              .get("payloadId")
              .asText();

      assertThat(payloadId).isNotEmpty();
    }

    waitFor(500);

    final Call getPayloadRequest = createEngineCall(createGetPayloadRequest(payloadId));

    final ObjectNode executionPayload;
    final ArrayNode executionRequests;
    final String blockAccessList;
    final String newBlockHash;
    final String getPayloadResponseBody;
    try (final Response getPayloadResponse = getPayloadRequest.execute()) {
      assertThat(getPayloadResponse.code()).isEqualTo(200);

      getPayloadResponseBody = getPayloadResponse.body().string();
      JsonNode responseJson = mapper.readTree(getPayloadResponseBody);
      JsonNode result = responseJson.get("result");

      if (result == null) {
        JsonNode error = responseJson.get("error");
        String errorMsg = error != null ? error.toString() : "Unknown error";
        throw new RuntimeException("engine_getPayloadV6 returned null result. Error: " + errorMsg);
      }

      executionPayload = (ObjectNode) result.get("executionPayload");
      JsonNode executionRequestsNode = result.get("executionRequests");
      executionRequests =
          (executionRequestsNode != null && !executionRequestsNode.isNull())
              ? (ArrayNode) executionRequestsNode
              : mapper.createArrayNode();

      // Extract blockAccessList from executionPayload for Amsterdam (required by newPayloadV5)
      JsonNode blockAccessListNode = executionPayload.get("blockAccessList");
      blockAccessList =
          (blockAccessListNode != null && !blockAccessListNode.isNull())
              ? blockAccessListNode.asText()
              : null;

      newBlockHash = executionPayload.get("blockHash").asText();

      assertThat(newBlockHash).isNotEmpty();

      // Verify the slot number in the execution payload matches what was requested
      JsonNode slotNumberNode = executionPayload.get("slotNumber");
      assertThat(slotNumberNode)
          .withFailMessage("executionPayload missing slotNumber field")
          .isNotNull();
      assertThat(Long.decode(slotNumberNode.asText()))
          .withFailMessage(
              "Expected slotNumber 0x%x in payload but got %s", slotNumber, slotNumberNode.asText())
          .isEqualTo(slotNumber);
    }

    String newPayloadRequestBody =
        createNewPayloadRequest(
            executionPayload.toString(),
            PARENT_BEACON_BLOCK_ROOT_TEST,
            executionRequests.toString());
    final Call newPayloadRequest = createEngineCall(newPayloadRequestBody);
    try (final Response newPayloadResponse = newPayloadRequest.execute()) {
      assertThat(newPayloadResponse.code()).isEqualTo(200);

      String newPayloadResponseBody = newPayloadResponse.body().string();
      JsonNode responseJson = mapper.readTree(newPayloadResponseBody);
      JsonNode result = responseJson.get("result");
      if (result == null) {
        JsonNode error = responseJson.get("error");
        String errorMsg = error != null ? error.toString() : "Unknown error";
        throw new RuntimeException(
            "engine_newPayloadV5 returned null result. Response: "
                + newPayloadResponseBody
                + ". Error: "
                + errorMsg);
      }
      final String responseStatus = result.get("status").asText();
      if (!"VALID".equals(responseStatus)) {
        JsonNode validationError = result.get("validationError");
        String errorMsg =
            validationError != null ? validationError.asText() : "No validation error";
        throw new AssertionError(
            "Expected VALID but was "
                + responseStatus
                + ". Validation error: "
                + errorMsg
                + "\n\n=== getPayloadV6 response ===\n"
                + getPayloadResponseBody
                + "\n\n=== blockAccessList extracted ===\n"
                + blockAccessList
                + "\n\n=== newPayloadV5 response ===\n"
                + newPayloadResponseBody);
      }
      assertThat(responseStatus).isEqualTo("VALID");
    }

    final Call moveChainAheadRequest = createEngineCall(createForkChoiceRequest(newBlockHash));

    try (final Response moveChainAheadResponse = moveChainAheadRequest.execute()) {
      assertThat(moveChainAheadResponse.code()).isEqualTo(200);
    }
  }

  /**
   * Sends a payload-building {@code engine_forkchoiceUpdatedV4} whose payload attributes omit the
   * {@code targetGasLimit} field, which is mandatory from Amsterdam onwards, and returns the parsed
   * JSON-RPC response so callers can assert the error.
   *
   * @return the parsed JSON-RPC response
   * @throws IOException if the engine call fails
   */
  public JsonNode forkChoiceUpdatedWithoutTargetGasLimit() throws IOException {
    final EthBlock.Block block = besuNode.execute(ethTransactions.block());

    blockTimeStamp += 1;
    slotNumber += 1;
    final Call request =
        createEngineCall(
            createForkChoiceRequest(block.getHash(), blockTimeStamp, slotNumber, false));

    try (final Response response = request.execute()) {
      assertThat(response.code()).isEqualTo(200);
      return mapper.readTree(response.body().string());
    }
  }

  private Call createEngineCall(final String request) {
    return httpClient.newCall(
        new Request.Builder()
            .url(besuNode.engineRpcUrl().get())
            .post(RequestBody.create(request, MEDIA_TYPE_JSON))
            .build());
  }

  private String createForkChoiceRequest(final String blockHash) {
    return createForkChoiceRequest(blockHash, null, null);
  }

  private String createForkChoiceRequest(
      final String parentBlockHash, final Long timeStamp, final Long slotNum) {
    return createForkChoiceRequest(parentBlockHash, timeStamp, slotNum, true);
  }

  private String createForkChoiceRequest(
      final String parentBlockHash,
      final Long timeStamp,
      final Long slotNum,
      final boolean includeTargetGasLimit) {
    final Optional<Long> maybeTimeStamp = Optional.ofNullable(timeStamp);
    final Optional<Long> maybeSlotNum = Optional.ofNullable(slotNum);

    String forkChoiceRequest =
        "{"
            + "  \"jsonrpc\": \"2.0\","
            + "  \"method\": \"engine_forkchoiceUpdatedV4\","
            + "  \"params\": ["
            + "    {"
            + "      \"headBlockHash\": \""
            + parentBlockHash
            + "\","
            + "      \"safeBlockHash\": \""
            + parentBlockHash
            + "\","
            + "      \"finalizedBlockHash\": \""
            + parentBlockHash
            + "\""
            + "    }";

    if (maybeTimeStamp.isPresent()) {
      forkChoiceRequest +=
          "    ,{"
              + "      \"timestamp\": \""
              + Long.toHexString(maybeTimeStamp.get())
              + "\","
              + "      \"prevRandao\": \"0x0000000000000000000000000000000000000000000000000000000000000000\","
              + "      \"suggestedFeeRecipient\": \"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b\","
              + "      \"withdrawals\": [],"
              + "      \"parentBeaconBlockRoot\": \"0x0000000000000000000000000000000000000000000000000000000000000000\","
              + "      \"slotNumber\": \""
              + (maybeSlotNum.isPresent() ? "0x" + Long.toHexString(maybeSlotNum.get()) : "0x0")
              + "\""
              + (includeTargetGasLimit ? ",      \"targetGasLimit\": \"0x1c9c380\"" : "")
              + "    }";
    }

    forkChoiceRequest += "  ]," + "  \"id\": 67" + "}";

    return forkChoiceRequest;
  }

  private String createGetPayloadRequest(final String payloadId) {
    return "{"
        + "  \"jsonrpc\": \"2.0\","
        + "  \"method\": \"engine_getPayloadV6\","
        + "  \"params\": [\""
        + payloadId
        + "\"],"
        + "  \"id\": 67"
        + "}";
  }

  private String createNewPayloadRequest(
      final String executionPayload,
      final String parentBeaconBlockRoot,
      final String executionRequests) {
    // engine_newPayloadV5 params: [executionPayload, versionedHashes, parentBeaconBlockRoot,
    // executionRequests]
    return "{"
        + "  \"jsonrpc\": \"2.0\","
        + "  \"method\": \"engine_newPayloadV5\","
        + "  \"params\": ["
        + executionPayload
        + ",[],"
        + "\""
        + parentBeaconBlockRoot
        + "\""
        + ","
        + executionRequests
        + "],"
        + "  \"id\": 67"
        + "}";
  }

  private static void waitFor(final long durationMilliSeconds) {
    try {
      Thread.sleep(durationMilliSeconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
