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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugTraceBlockTest {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/api/jsonrpc/trace/chain-data/genesis-osaka.json";
  private static final KeyPair KEY_PAIR =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"),
                  SignatureAlgorithm.ALGORITHM));
  private static final Address CONTRACT_ADDRESS =
      Address.fromHexString("0x0030000000000000000000000000000000000000");

  private DebugTraceBlock debugTraceBlock;
  private ExecutionContextTestFixture fixture;
  private Block testBlock;
  private Transaction testTransaction;
  private final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

  @BeforeEach
  public void setUp() {
    final GenesisConfig genesisConfig = GenesisConfig.fromResource(GENESIS_RESOURCE);
    fixture =
        ExecutionContextTestFixture.builder(genesisConfig)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();

    final BlockchainQueries blockchainQueries =
        new BlockchainQueries(
            fixture.getProtocolSchedule(),
            fixture.getBlockchain(),
            fixture.getStateArchive(),
            MiningConfiguration.MINING_DISABLED);

    debugTraceBlock = new DebugTraceBlock(fixture.getProtocolSchedule(), blockchainQueries);

    // Build a signed EIP-1559 transaction calling the increment contract with input=5
    testTransaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(100_000L)
            .to(CONTRACT_ADDRESS)
            .value(Wei.ZERO)
            .payload(Bytes32.leftPad(Bytes.of(5)))
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);

    // Build a block whose parent is the genesis block
    final BlockHeader genesis = fixture.getBlockchain().getChainHeadHeader();
    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(genesis.getNumber() + 1L)
            .parentHash(genesis.getHash())
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();
    final BlockBody blockBody =
        new BlockBody(List.of(testTransaction), Collections.emptyList(), Optional.empty());
    testBlock = new Block(blockHeader, blockBody);
  }

  @Test
  public void nameShouldBeDebugTraceBlock() {
    assertThat(debugTraceBlock.getName()).isEqualTo("debug_traceBlock");
  }

  @Test
  public void shouldReturnCorrectResponse() throws IOException {
    final Object[] params = new Object[] {testBlock.toRlp().toString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceBlock", params));

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    debugTraceBlock.streamResponse(request, out, mapper);
    final String json = out.toString(UTF_8);
    assertThat(json).startsWith("{\"jsonrpc\":\"2.0\"");
    assertThat(json).contains("\"result\":");

    final JsonNode response = mapper.readTree(json);
    assertThat(response.has("result")).isTrue();
    final JsonNode result = response.get("result");
    assertThat(result.isArray()).isTrue();
    assertThat(result.size()).isEqualTo(1);

    final JsonNode txTrace = result.get(0);
    assertThat(txTrace.has("txHash")).isTrue();
    assertThat(txTrace.get("txHash").asText()).isEqualTo(testTransaction.getHash().toHexString());

    final JsonNode traceResult = txTrace.get("result");
    assertThat(traceResult.get("failed").asBoolean()).isFalse();
    assertThat(traceResult.get("gas").asLong()).isGreaterThan(0);
    // Contract increments input (5) by 1, returns 6 as 32-byte value
    assertThat(traceResult.get("returnValue").asText())
        .isEqualTo("0x0000000000000000000000000000000000000000000000000000000000000006");

    // Verify structLogs contains the expected opcode sequence
    final JsonNode structLogs = traceResult.get("structLogs");
    assertThat(structLogs.isArray()).isTrue();
    assertThat(structLogs.size()).isEqualTo(9);

    // First opcode: PUSH1 0x00
    assertThat(structLogs.get(0).get("pc").asInt()).isEqualTo(0);
    assertThat(structLogs.get(0).get("op").asText()).isEqualTo("PUSH1");
    assertThat(structLogs.get(0).get("depth").asInt()).isEqualTo(1);
    assertThat(structLogs.get(0).get("gas").asLong()).isGreaterThan(0);
    assertThat(structLogs.get(0).get("gasCost").asLong()).isGreaterThan(0);

    // Verify the full opcode sequence
    final String[] expectedOps = {
      "PUSH1", "CALLDATALOAD", "PUSH1", "ADD", "PUSH1", "MSTORE", "PUSH1", "PUSH1", "RETURN"
    };
    for (int i = 0; i < expectedOps.length; i++) {
      assertThat(structLogs.get(i).get("op").asText()).isEqualTo(expectedOps[i]);
    }
  }

  @Test
  public void batchResponseShouldMatchStreamingOutput() throws IOException {
    final Object[] params = new Object[] {testBlock.toRlp().toString()};

    // batch (accumulating) path
    final JsonRpcRequestContext batchRequest =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceBlock", params));
    final JsonRpcResponse response = debugTraceBlock.response(batchRequest);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Object batchResult = ((JsonRpcSuccessResponse) response).getResult();
    assertThat(batchResult).isNotNull();
    final JsonNode batchJson = mapper.readTree(mapper.writeValueAsBytes(batchResult));

    // streaming path
    final JsonRpcRequestContext streamRequest =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceBlock", params));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    debugTraceBlock.streamResponse(streamRequest, out, mapper);
    final JsonNode streamJson = mapper.readTree(out.toByteArray()).get("result");

    assertThat(batchJson)
        .as("batch (accumulating) and streaming paths must produce identical result JSON")
        .isEqualTo(streamJson);
  }

  @Test
  public void shouldReturnErrorResponseWhenParentBlockMissing() throws IOException {
    // Create a block whose parent doesn't exist in the blockchain
    final Block orphanBlock =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    final Object[] params = new Object[] {orphanBlock.toRlp().toString()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceBlock", params));

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    debugTraceBlock.streamResponse(request, out, mapper);
    final JsonNode response = mapper.readTree(out.toByteArray());
    assertThat(response.has("error")).isTrue();
    assertThat(response.get("error").get("message").asText()).contains("Parent block not found");
  }

  @Test
  public void shouldHandleInvalidParametersGracefully() throws IOException {
    final Object[] invalidParams = new Object[] {"invalid RLP"};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceBlock", invalidParams));

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    debugTraceBlock.streamResponse(request, out, mapper);
    final JsonNode response = mapper.readTree(out.toByteArray());
    assertThat(response.has("error")).isTrue();
    assertThat(response.get("error").get("message").asText())
        .contains("Invalid block param (block not found)");
  }
}
