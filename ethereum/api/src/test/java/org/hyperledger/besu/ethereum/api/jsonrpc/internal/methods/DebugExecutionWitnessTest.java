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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionWitnessResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DebugExecutionWitnessTest {

  @Test
  public void nameShouldBeDebugExecutionWitness() {
    final DebugExecutionWitness method =
        new DebugExecutionWitness(
            mock(BlockchainQueries.class),
            mock(ProtocolContext.class),
            mock(ProtocolSchedule.class));
    assertThat(method.getName()).isEqualTo("debug_executionWitness");
  }

  @Test
  public void shouldReturnWitnessForChainHead() {
    // End-to-end: real chain imported; DebugExecutionWitness re-executes the requested block
    // against the persisted parent state to rebuild the witness.
    final BlockchainSetupUtil setup = BlockchainSetupUtil.forHiveTesting(DataStorageFormat.BONSAI);
    setup.importAllBlocks();

    final BlockchainQueries queries =
        new BlockchainQueries(
            setup.getProtocolSchedule(),
            setup.getBlockchain(),
            setup.getWorldArchive(),
            MiningConfiguration.MINING_DISABLED);

    final Hash chainHeadHash = setup.getBlockchain().getChainHeadHash();
    final JsonRpcRequestContext request = requestForBlockHash(chainHeadHash);

    final DebugExecutionWitness method =
        new DebugExecutionWitness(queries, setup.getProtocolContext(), setup.getProtocolSchedule());
    final Object result = method.response(request);

    assertThat(result).isInstanceOf(JsonRpcSuccessResponse.class);
    final ExecutionWitnessResult witness =
        (ExecutionWitnessResult) ((JsonRpcSuccessResponse) result).getResult();
    assertThat(witness).isNotNull();
    assertThat(witness.state()).isNotEmpty();
    assertThat(witness.headers()).isNotEmpty();
    witness.state().forEach(node -> assertThat(node).startsWith("0x"));
    witness.codes().forEach(code -> assertThat(code).startsWith("0x"));
    witness.headers().forEach(header -> assertThat(header).startsWith("0x"));
    assertThat(witness.state()).isSortedAccordingTo(String::compareTo);
    assertThat(witness.codes()).isSortedAccordingTo(String::compareTo);
  }

  @Test
  public void shouldReturnBlockNotFoundForGenesis() {
    // Genesis has no parent header on-chain, so the re-execution path can't run and we surface
    // BLOCK_NOT_FOUND.
    final BlockchainSetupUtil setup = BlockchainSetupUtil.forHiveTesting(DataStorageFormat.BONSAI);
    setup.importAllBlocks();

    final BlockchainQueries queries =
        new BlockchainQueries(
            setup.getProtocolSchedule(),
            setup.getBlockchain(),
            setup.getWorldArchive(),
            MiningConfiguration.MINING_DISABLED);

    final Hash genesisHash = setup.getBlockchain().getGenesisBlock().getHash();
    final JsonRpcRequestContext request = requestForBlockHash(genesisHash);

    final DebugExecutionWitness method =
        new DebugExecutionWitness(queries, setup.getProtocolContext(), setup.getProtocolSchedule());
    final Object result = method.response(request);

    assertThat(result).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) result).getErrorType())
        .isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  private static JsonRpcRequestContext requestForBlockHash(final Hash blockHash) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0", "debug_executionWitness", new Object[] {blockHash.toHexString()}));
  }
}
