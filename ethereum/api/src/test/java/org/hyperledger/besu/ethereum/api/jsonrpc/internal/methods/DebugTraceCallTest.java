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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;

import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugTraceCallTest {

  private static final Hash UNKNOWN_BLOCK_HASH = Hash.hash(Bytes.of(1));
  private static final Hash KNOWN_BLOCK_HASH = Hash.hash(Bytes.of(2));
  private static final long KNOWN_BLOCK_NUMBER = 5L;

  private BlockchainQueries blockchainQueries;
  private ProtocolSchedule protocolSchedule;
  private TransactionSimulator transactionSimulator;
  private DebugTraceCall method;

  @BeforeEach
  public void setUp() {
    blockchainQueries = mock(BlockchainQueries.class);
    protocolSchedule = mock(ProtocolSchedule.class);
    transactionSimulator = mock(TransactionSimulator.class);
    method = new DebugTraceCall(blockchainQueries, protocolSchedule, transactionSimulator);
  }

  @Test
  void blockHashParamReturnsBlockNotFoundForUnknownHash() {
    when(blockchainQueries.getBlockHeaderByHash(UNKNOWN_BLOCK_HASH)).thenReturn(Optional.empty());

    final JsonRpcRequestContext request = requestWithBlockHash(UNKNOWN_BLOCK_HASH.toHexString());

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  @Test
  void blockHashParamResolvesToCorrectBlockNumber() {
    final BlockHeader header =
        new BlockHeaderTestFixture().number(KNOWN_BLOCK_NUMBER).buildHeader();
    when(blockchainQueries.getBlockHeaderByHash(KNOWN_BLOCK_HASH)).thenReturn(Optional.of(header));
    when(blockchainQueries.getBlockHeaderByNumber(KNOWN_BLOCK_NUMBER))
        .thenReturn(Optional.of(header));
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(transactionSimulator.process(any(), any(), any(), any(), any(), eq(header)))
        .thenReturn(Optional.empty());

    final JsonRpcRequestContext request = requestWithBlockHash(KNOWN_BLOCK_HASH.toHexString());

    final JsonRpcResponse response = method.response(request);

    // TransactionSimulator returned empty → INTERNAL_ERROR, not BLOCK_NOT_FOUND.
    // This confirms the hash was resolved to the correct block and simulation was attempted.
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isNotEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  @Test
  void requireCanonicalReturnErrorForNonCanonicalBlockHash() {
    final BlockHeader header =
        new BlockHeaderTestFixture().number(KNOWN_BLOCK_NUMBER).buildHeader();
    when(blockchainQueries.getBlockHeaderByHash(KNOWN_BLOCK_HASH)).thenReturn(Optional.of(header));
    when(blockchainQueries.blockIsOnCanonicalChain(header.getBlockHash())).thenReturn(false);

    final JsonRpcRequestContext request =
        requestWithBlockHashObject(KNOWN_BLOCK_HASH.toHexString(), true);

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.JSON_RPC_NOT_CANONICAL_ERROR);
  }

  @Test
  void eip1898BlockNumberObjectFormResolvesCorrectly() {
    final BlockHeader header =
        new BlockHeaderTestFixture().number(KNOWN_BLOCK_NUMBER).buildHeader();
    when(blockchainQueries.getBlockHeaderByNumber(KNOWN_BLOCK_NUMBER))
        .thenReturn(Optional.of(header));
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(transactionSimulator.process(any(), any(), any(), any(), any(), eq(header)))
        .thenReturn(Optional.empty());

    final JsonRpcRequestContext request =
        requestWithParam(Map.of("blockNumber", "0x" + Long.toHexString(KNOWN_BLOCK_NUMBER)));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isNotEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  private JsonRpcRequestContext requestWithBlockHash(final String blockHash) {
    return requestWithParam(blockHash);
  }

  private JsonRpcRequestContext requestWithBlockHashObject(
      final String blockHash, final boolean requireCanonical) {
    return requestWithParam(Map.of("blockHash", blockHash, "requireCanonical", requireCanonical));
  }

  private JsonRpcRequestContext requestWithParam(final Object blockParam) {
    final var callParams =
        ImmutableCallParameter.builder().to(Address.fromHexString("0x1234")).build();
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "debug_traceCall", new Object[] {callParams, blockParam}));
  }
}
