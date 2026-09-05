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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestingCommitBlockV1Test {

  private static final long DEFAULT_TIMESTAMP = 100L;

  @Mock private ProtocolContext protocolContext;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private MiningConfiguration miningConfiguration;
  @Mock private TransactionPool transactionPool;
  @Mock private EthScheduler ethScheduler;
  @Mock private MutableBlockchain blockchain;
  @Mock private Block genesisBlock;
  @Mock private BlockHeader genesisHeader;

  private TestingCommitBlockV1 method;

  @BeforeEach
  void setUp() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getGenesisBlock()).thenReturn(genesisBlock);
    when(genesisBlock.getHeader()).thenReturn(genesisHeader);
    when(genesisHeader.getGasLimit()).thenReturn(30_000_000L);
    method =
        new TestingCommitBlockV1(
            protocolContext, protocolSchedule, miningConfiguration, transactionPool, ethScheduler);
  }

  @Test
  void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(RpcMethod.TESTING_COMMIT_BLOCK_V1.getMethodName());
  }

  @Test
  void shouldReturnErrorForZeroTimestamp() {
    // timestamp = 0 is caught by validatePayloadAttributes; the blockchain is never queried.
    final JsonRpcResponse response =
        method.response(buildRequest(attributesWithTimestamp("0x0"), new String[0], null));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_PARAMS);
    verifyNoInteractions(blockchain);
  }

  @Test
  void shouldReturnErrorForMalformedTransaction() {
    // Valid attributes, bad tx hex: error before the block is built, blockchain not touched.
    final JsonRpcResponse response =
        method.response(
            buildRequest(validAttributes(DEFAULT_TIMESTAMP), new String[] {"0xdeadbeef"}, null));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_TRANSACTION_PARAMS);
    verifyNoInteractions(blockchain);
  }

  // ---- helpers ----

  private Map<String, Object> validAttributes(final long timestamp) {
    final Map<String, Object> attrs = new LinkedHashMap<>();
    attrs.put("timestamp", Bytes.ofUnsignedLong(timestamp).toQuantityHexString());
    attrs.put("prevRandao", Bytes32.ZERO.toHexString());
    attrs.put("suggestedFeeRecipient", "0x0000000000000000000000000000000000000000");
    attrs.put("withdrawals", Collections.emptyList());
    attrs.put("parentBeaconBlockRoot", Bytes32.ZERO.toHexString());
    return attrs;
  }

  private Map<String, Object> attributesWithTimestamp(final String timestampHex) {
    final Map<String, Object> attrs = new LinkedHashMap<>();
    attrs.put("timestamp", timestampHex);
    attrs.put("prevRandao", Bytes32.ZERO.toHexString());
    attrs.put("suggestedFeeRecipient", "0x0000000000000000000000000000000000000000");
    attrs.put("withdrawals", Collections.emptyList());
    attrs.put("parentBeaconBlockRoot", Bytes32.ZERO.toHexString());
    return attrs;
  }

  private JsonRpcRequestContext buildRequest(
      final Map<String, Object> payloadAttributes,
      final Object transactions,
      final Object extraData) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0",
            RpcMethod.TESTING_COMMIT_BLOCK_V1.getMethodName(),
            new Object[] {payloadAttributes, transactions, extraData}));
  }
}
