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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugReplayBlockTest {
  private static final long BLOCK_NUMBER = 2L;
  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private final Block block =
      blockDataGenerator.block(new BlockOptions().setBlockNumber(BLOCK_NUMBER));
  private final Hash originalHead = block.getHeader().getParentHash();

  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final Blockchain queriesBlockchain = mock(Blockchain.class);
  private final MutableBlockchain mutableBlockchain = mock(MutableBlockchain.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final BlockValidator blockValidator = mock(BlockValidator.class);
  private DebugReplayBlock method;

  @BeforeEach
  public void setUp() {
    when(blockchainQueries.getBlockchain()).thenReturn(queriesBlockchain);
    when(queriesBlockchain.getBlockByNumber(BLOCK_NUMBER)).thenReturn(Optional.of(block));

    when(protocolContext.getBlockchain()).thenReturn(mutableBlockchain);
    when(mutableBlockchain.getChainHeadHash()).thenReturn(originalHead);

    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSchedule.getByBlockHeader(block.getHeader())).thenReturn(protocolSpec);
    when(protocolSpec.getBlockValidator()).thenReturn(blockValidator);

    method = new DebugReplayBlock(blockchainQueries, protocolContext, protocolSchedule);
  }

  @Test
  public void shouldReturnUnknownBlockWhenBlockIsMissing() {
    when(queriesBlockchain.getBlockByNumber(BLOCK_NUMBER)).thenReturn(Optional.empty());

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) request();

    assertThat(response.getErrorType()).isEqualTo(RpcErrorType.UNKNOWN_BLOCK);
    verify(mutableBlockchain, never()).rewindToBlock(BLOCK_NUMBER - 1);
  }

  @Test
  public void shouldForwardToReplayedBlockOnSuccess() {
    when(blockValidator.validateAndProcessBlock(
            any(), eq(block), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(new BlockProcessingResult(Optional.empty()));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) request();

    assertThat(response.getResult()).isEqualTo(JsonRpcSuccessResponse.SUCCESS_RESULT);
    verify(mutableBlockchain).rewindToBlock(BLOCK_NUMBER - 1);
    verify(mutableBlockchain).forwardToBlock(block.getHeader());
    // a successful replay keeps the new head, it is not rolled back
    verify(mutableBlockchain, never()).rewindToBlock(originalHead);
  }

  @Test
  public void shouldReturnErrorAndRestoreHeadOnFailedResult() {
    when(blockValidator.validateAndProcessBlock(
            any(), eq(block), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(BlockProcessingResult.worldStateUnavailable("parent state missing"));

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) request();

    assertThat(response.getErrorType()).isEqualTo(RpcErrorType.BLOCK_IMPORT_ERROR);
    verify(mutableBlockchain).rewindToBlock(originalHead);
    verify(mutableBlockchain, never()).forwardToBlock(any());
  }

  @Test
  public void shouldReturnErrorAndRestoreHeadOnException() {
    when(blockValidator.validateAndProcessBlock(
            any(), eq(block), any(), any(), any(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("boom"));

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) request();

    assertThat(response.getErrorType()).isEqualTo(RpcErrorType.INTERNAL_ERROR);
    verify(mutableBlockchain).rewindToBlock(originalHead);
    verify(mutableBlockchain, never()).forwardToBlock(any());
  }

  private JsonRpcResponse request() {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "debug_replayBlock", new Object[] {"0x" + Long.toHexString(BLOCK_NUMBER)})));
  }
}
