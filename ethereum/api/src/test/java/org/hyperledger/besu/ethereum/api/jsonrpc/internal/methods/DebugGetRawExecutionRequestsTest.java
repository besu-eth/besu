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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugGetRawExecutionRequestsTest {
  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private final Block block =
      blockDataGenerator.block(new BlockOptions().setRequestsHash(Hash.ZERO));
  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final BlockValidator blockValidator = mock(BlockValidator.class);
  private DebugGetRawExecutionRequests method;

  @BeforeEach
  public void setUp() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchainQueries.getBlockHeaderByHash(block.getHash()))
        .thenReturn(Optional.of(block.getHeader()));
    when(blockchain.getBlockByHash(block.getHash())).thenReturn(Optional.of(block));
    when(blockchain.getBlockHeader(block.getHeader().getParentHash()))
        .thenReturn(Optional.of(blockDataGenerator.header()));

    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSchedule.getByBlockHeader(block.getHeader())).thenReturn(protocolSpec);
    when(protocolSpec.getBlockValidator()).thenReturn(blockValidator);

    method =
        new DebugGetRawExecutionRequests(
            blockchainQueries, mock(ProtocolContext.class), protocolSchedule);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("debug_getRawExecutionRequests");
  }

  @Test
  public void shouldReturnRequestsInCanonicalForm() {
    final Request consolidation =
        new Request(RequestType.CONSOLIDATION, Bytes.fromHexString("0x03"));
    final Request deposit = new Request(RequestType.DEPOSIT, Bytes.fromHexString("0x0102"));
    final Request emptyWithdrawals = new Request(RequestType.WITHDRAWAL, Bytes.EMPTY);
    reExecutionYields(Optional.of(List.of(consolidation, emptyWithdrawals, deposit)));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) request();

    // Sorted by type, empty types dropped: the form requests_hash commits to.
    assertThat(response.getResult()).isEqualTo(List.of(deposit, consolidation));
  }

  @Test
  public void shouldReturnNullBeforePragueWithoutReExecuting() {
    final Block prePrague = blockDataGenerator.block();
    when(blockchainQueries.getBlockHeaderByHash(prePrague.getHash()))
        .thenReturn(Optional.of(prePrague.getHeader()));
    when(blockchain.getBlockByHash(prePrague.getHash())).thenReturn(Optional.of(prePrague));
    // No parent state either: a pre-Prague block must not need it.
    when(blockchain.getBlockHeader(prePrague.getHeader().getParentHash()))
        .thenReturn(Optional.empty());

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) request(prePrague);

    assertThat(response.getResult()).isNull();
    verifyNoInteractions(blockValidator);
  }

  @Test
  public void shouldReturnEmptyListForPragueGenesisWithoutReExecuting() {
    final Block genesis =
        blockDataGenerator.block(new BlockOptions().setBlockNumber(0).setRequestsHash(Hash.ZERO));
    when(blockchainQueries.getBlockHeaderByHash(genesis.getHash()))
        .thenReturn(Optional.of(genesis.getHeader()));
    when(blockchain.getBlockByHash(genesis.getHash())).thenReturn(Optional.of(genesis));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) request(genesis);

    assertThat(response.getResult()).isEqualTo(List.of());
    verifyNoInteractions(blockValidator);
  }

  @Test
  public void shouldReturnInternalErrorWhenReExecutionFails() {
    when(blockValidator.validateAndProcessBlock(
            any(), eq(block), any(), any(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(BlockProcessingResult.FAILED);

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) request();

    assertThat(response.getErrorType()).isEqualTo(RpcErrorType.INTERNAL_ERROR);
  }

  @Test
  public void shouldReturnBlockNotFoundWhenParentIsMissing() {
    when(blockchain.getBlockHeader(block.getHeader().getParentHash())).thenReturn(Optional.empty());

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) request();

    assertThat(response.getErrorType()).isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  private void reExecutionYields(final Optional<List<Request>> requests) {
    final BlockProcessingOutputs outputs =
        new BlockProcessingOutputs(mock(MutableWorldState.class), List.of(), requests);
    when(blockValidator.validateAndProcessBlock(
            any(), eq(block), any(), any(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(new BlockProcessingResult(Optional.of(outputs)));
  }

  private JsonRpcResponse request() {
    return request(block);
  }

  private JsonRpcResponse request(final Block target) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                "debug_getRawExecutionRequests",
                new Object[] {target.getHash().toHexString()})));
  }
}
