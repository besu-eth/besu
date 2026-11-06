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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DebugExecutionWitnessTest {

  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);

  @BeforeEach
  void setUp() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
  }

  @Test
  void blockBeforeOsakaIsUnsupported() {
    assertThat(errorFor(PRAGUE)).isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
  }

  @Test
  void blockOfNonMainnetForkIsUnsupported() {
    assertThat(errorFor(mock(HardforkId.class))).isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
  }

  @Test
  void osakaBlockIsSupported() {
    // Past the fork check, the request fails only because the test chain has no parent block.
    assertThat(errorFor(OSAKA)).isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  @Test
  void amsterdamBlockOnChainWithoutOsakaMilestoneIsSupported() {
    // A genesis that schedules Amsterdam without Osaka has no Osaka milestone.
    when(protocolSchedule.milestoneFor(OSAKA)).thenReturn(Optional.empty());

    assertThat(errorFor(AMSTERDAM)).isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  private RpcErrorType errorFor(final HardforkId blockFork) {
    final Block block = blockDataGenerator.block();
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSpec.getHardforkId()).thenReturn(blockFork);
    when(protocolSchedule.getByBlockHeader(block.getHeader())).thenReturn(protocolSpec);
    final Hash blockHash = block.getHash();
    when(blockchainQueries.getBlockHeaderByHash(blockHash))
        .thenReturn(Optional.of(block.getHeader()));
    when(blockchain.getBlockByHash(blockHash)).thenReturn(Optional.of(block));

    final DebugExecutionWitness method =
        new DebugExecutionWitness(blockchainQueries, mock(ProtocolContext.class), protocolSchedule);
    final JsonRpcResponse response =
        method.response(
            new JsonRpcRequestContext(
                new JsonRpcRequest(
                    "2.0", "debug_executionWitness", new Object[] {blockHash.toHexString()})));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    return ((JsonRpcErrorResponse) response).getErrorType();
  }
}
