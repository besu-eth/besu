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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs trace_call and trace_callMany against a real simulator at a block with a base fee, and
 * compares their fee handling with eth_call.
 */
public class TraceCallFeesTest {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/api/jsonrpc/trace/chain-data/genesis-osaka.json";
  private static final String SENDER = "0x627306090abab3a6e1400e9345bc60c78a8bef57";
  private static final String COINBASE = "0x0000000000000000000000000000000000000000";
  // genesis block, whose base fee is 7
  private static final String BLOCK = "0x0";
  private static final long BASE_FEE = 7;
  // init code returning BASEFEE and GASPRICE: BASEFEE PUSH0 MSTORE GASPRICE PUSH1 0x20 MSTORE
  // PUSH1 0x40 PUSH0 RETURN
  private static final String INIT_CODE = "0x485f523a60205260405ff3";

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

  private TransactionSimulator transactionSimulator;
  private EthCall ethCall;
  private TraceCall traceCall;
  private TraceCallMany traceCallMany;

  @BeforeEach
  public void setUp() {
    final ExecutionContextTestFixture fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final BlockchainQueries blockchainQueries =
        new BlockchainQueries(
            fixture.getProtocolSchedule(),
            fixture.getBlockchain(),
            fixture.getStateArchive(),
            MiningConfiguration.MINING_DISABLED);
    transactionSimulator =
        new TransactionSimulator(
            fixture.getBlockchain(),
            fixture.getStateArchive(),
            fixture.getProtocolSchedule(),
            MiningConfiguration.MINING_DISABLED,
            0L);

    ethCall = new EthCall(blockchainQueries, transactionSimulator, new NoOpMetricsSystem());
    traceCall =
        new TraceCall(blockchainQueries, fixture.getProtocolSchedule(), transactionSimulator);
    traceCallMany =
        new TraceCallMany(blockchainQueries, fixture.getProtocolSchedule(), transactionSimulator);
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"gasPrice\":\"0x0\",", "\"maxFeePerGas\":\"0x0\",", ""})
  public void unpricedTraceCallRunsWithZeroBaseFeeLikeEthCall(final String fees) throws Exception {
    final String call = call(fees);

    final String ethOutput = (String) success(ethCall, "eth_call", call + ",\"" + BLOCK + "\"");
    assertThat(ethOutput).isEqualTo(basefeeAndGasPrice(0, 0));

    final JsonNode trace =
        traceResult(success(traceCall, "trace_call", call + ",[\"trace\"],\"" + BLOCK + "\""));
    assertThat(trace.get("output").asText()).isEqualTo(ethOutput);
  }

  @Test
  public void pricedTraceCallSeesBlockBaseFeeLikeEthCall() throws Exception {
    final String call = call("\"gasPrice\":\"0x10\",");

    final String ethOutput = (String) success(ethCall, "eth_call", call + ",\"" + BLOCK + "\"");
    assertThat(ethOutput).isEqualTo(basefeeAndGasPrice(BASE_FEE, 0x10));

    final JsonNode trace =
        traceResult(success(traceCall, "trace_call", call + ",[\"trace\"],\"" + BLOCK + "\""));
    assertThat(trace.get("output").asText()).isEqualTo(ethOutput);
  }

  @Test
  public void underpricedTraceCallIsRejectedLikeEthCall() throws Exception {
    final String call = call("\"gasPrice\":\"0x1\",");

    final JsonRpcError ethError = error(ethCall, "eth_call", call + ",\"" + BLOCK + "\"");
    assertThat(ethError.getCode())
        .isEqualTo(RpcErrorType.GAS_PRICE_BELOW_CURRENT_BASE_FEE.getCode());

    final JsonRpcError traceError =
        error(traceCall, "trace_call", call + ",[\"trace\"],\"" + BLOCK + "\"");
    assertThat(traceError.getCode()).isEqualTo(ethError.getCode());
    assertThat(traceError.getMessage()).isEqualTo(ethError.getMessage());
  }

  @Test
  public void callManyAppliesEachCallsOwnPricingAndCarriesStateForward() throws Exception {
    final String types = "[\"trace\",\"stateDiff\"]";
    final String free = "[" + call("\"gasPrice\":\"0x0\",") + "," + types + "]";
    final String priced = "[" + call("\"gasPrice\":\"0x10\",") + "," + types + "]";

    final JsonNode results =
        mapper.valueToTree(
            success(
                traceCallMany,
                "trace_callMany",
                "[" + free + "," + priced + "," + free + "],\"" + BLOCK + "\""));
    assertThat(results.isArray()).isTrue();
    assertThat(results).hasSize(3);

    // each call sees the environment of its own price
    assertThat(results.get(0).get("output").asText()).isEqualTo(basefeeAndGasPrice(0, 0));
    assertThat(results.get(1).get("output").asText()).isEqualTo(basefeeAndGasPrice(BASE_FEE, 0x10));
    assertThat(results.get(2).get("output").asText()).isEqualTo(basefeeAndGasPrice(0, 0));

    // each call starts from the state left by the previous one
    for (int i = 0; i < 3; i++) {
      final JsonNode nonce = senderDiff(results.get(i)).get("nonce").get("*");
      assertThat(nonce.get("from").asText()).isEqualTo("0x" + i);
      assertThat(nonce.get("to").asText()).isEqualTo("0x" + (i + 1));
    }

    // unpriced calls have no fee effects
    assertThat(senderDiff(results.get(0)).get("balance").asText()).isEqualTo("=");
    assertThat(senderDiff(results.get(2)).get("balance").asText()).isEqualTo("=");

    // the priced call pays gasPrice for the gas it uses, and the coinbase receives the tip above
    // the base fee
    final BigInteger gasUsed = BigInteger.valueOf(pricedGasUsed(0x10));
    final JsonNode senderBalance = senderDiff(results.get(1)).get("balance").get("*");
    assertThat(quantity(senderBalance.get("from")).subtract(quantity(senderBalance.get("to"))))
        .isEqualTo(gasUsed.multiply(BigInteger.valueOf(0x10)));
    assertThat(balanceGain(results.get(1).get("stateDiff").get(COINBASE)))
        .isEqualTo(gasUsed.multiply(BigInteger.valueOf(0x10 - BASE_FEE)));
  }

  // the gas the call uses when simulated on its own
  private long pricedGasUsed(final long gasPrice) {
    return transactionSimulator
        .process(
            ImmutableCallParameter.builder()
                .sender(Address.fromHexString(SENDER))
                .input(Bytes.fromHexString(INIT_CODE))
                .gasPrice(Wei.of(gasPrice))
                .build(),
            TransactionValidationParams.transactionSimulatorAllowFutureNonce(),
            OperationTracer.NO_TRACING,
            0L)
        .orElseThrow()
        .getGasEstimate();
  }

  private static String call(final String fees) {
    return "{\"from\":\"" + SENDER + "\"," + fees + "\"data\":\"" + INIT_CODE + "\"}";
  }

  private static String basefeeAndGasPrice(final long baseFee, final long gasPrice) {
    return Bytes.concatenate(
            Bytes32.leftPad(Bytes.minimalBytes(baseFee)),
            Bytes32.leftPad(Bytes.minimalBytes(gasPrice)))
        .toHexString();
  }

  private JsonNode traceResult(final Object result) {
    return mapper.valueToTree(result);
  }

  private static JsonNode senderDiff(final JsonNode result) {
    return result.get("stateDiff").get(SENDER);
  }

  private static BigInteger balanceGain(final JsonNode accountDiff) {
    final JsonNode balance = accountDiff.get("balance");
    if (balance.has("+")) {
      return quantity(balance.get("+"));
    }
    return quantity(balance.get("*").get("to")).subtract(quantity(balance.get("*").get("from")));
  }

  private static BigInteger quantity(final JsonNode value) {
    return new BigInteger(value.asText().substring(2), 16);
  }

  private static Object success(final JsonRpcMethod method, final String name, final String params)
      throws Exception {
    final JsonRpcResponse response = method.response(request(name, params));
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    return ((JsonRpcSuccessResponse) response).getResult();
  }

  private static JsonRpcError error(
      final JsonRpcMethod method, final String name, final String params) throws Exception {
    final JsonRpcResponse response = method.response(request(name, params));
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    return ((JsonRpcErrorResponse) response).getError();
  }

  private static JsonRpcRequestContext request(final String name, final String params)
      throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"" + name + "\",\"params\":[" + params + "]}";
    return new JsonRpcRequestContext(new ObjectMapper().readValue(json, JsonRpcRequest.class));
  }
}
