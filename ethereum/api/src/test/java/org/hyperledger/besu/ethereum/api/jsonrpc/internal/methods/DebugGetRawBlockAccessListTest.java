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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugGetRawBlockAccessListTest {
  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private DebugGetRawBlockAccessList method;
  private BlockchainQueries blockchainQueries;
  private Blockchain blockchain;

  @BeforeEach
  public void setUp() {
    blockchainQueries = mock(BlockchainQueries.class);
    blockchain = mock(Blockchain.class);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    method = new DebugGetRawBlockAccessList(blockchainQueries);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("debug_getRawBlockAccessList");
  }

  @Test
  public void shouldReturnRlpEncodedBlockAccessList() {
    final long blockNumber = 5L;
    final BlockHeader header = blockDataGenerator.header(blockNumber);
    final Hash blockHash = header.getHash();

    when(blockchainQueries.getBlockHeaderByNumber(blockNumber)).thenReturn(Optional.of(header));
    when(blockchainQueries.isBlockAccessListSupported(header)).thenReturn(true);
    when(blockchain.getBlockAccessList(blockHash))
        .thenReturn(Optional.of(new BlockAccessList(Collections.emptyList())));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse)
            method.response(
                new JsonRpcRequestContext(
                    new JsonRpcRequest(
                        "2.0",
                        method.getName(),
                        new Object[] {String.format("0x%X", blockNumber)})));

    assertThat(response.getResult()).isEqualTo("0xc0");
  }

  @Test
  public void shouldReturnResourceNotFoundForPreAmsterdamBlocks() {
    final long blockNumber = 5L;
    final BlockHeader header = blockDataGenerator.header(blockNumber);

    when(blockchainQueries.getBlockHeaderByNumber(blockNumber)).thenReturn(Optional.of(header));
    when(blockchainQueries.isBlockAccessListSupported(header)).thenReturn(false);

    final JsonRpcErrorResponse response =
        (JsonRpcErrorResponse)
            method.response(
                new JsonRpcRequestContext(
                    new JsonRpcRequest(
                        "2.0",
                        method.getName(),
                        new Object[] {String.format("0x%X", blockNumber)})));

    assertThat(response.getErrorType()).isEqualTo(RpcErrorType.RESOURCE_NOT_FOUND);
  }

  @Test
  public void shouldReturnPrunedHistoryUnavailableWhenAccessListIsMissing() {
    final long blockNumber = 5L;
    final BlockHeader header = blockDataGenerator.header(blockNumber);
    final Hash blockHash = header.getHash();

    when(blockchainQueries.getBlockHeaderByNumber(blockNumber)).thenReturn(Optional.of(header));
    when(blockchainQueries.isBlockAccessListSupported(header)).thenReturn(true);
    when(blockchain.getBlockAccessList(blockHash)).thenReturn(Optional.empty());

    final JsonRpcErrorResponse response =
        (JsonRpcErrorResponse)
            method.response(
                new JsonRpcRequestContext(
                    new JsonRpcRequest(
                        "2.0",
                        method.getName(),
                        new Object[] {String.format("0x%X", blockNumber)})));

    assertThat(response.getErrorType()).isEqualTo(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
  }
}
