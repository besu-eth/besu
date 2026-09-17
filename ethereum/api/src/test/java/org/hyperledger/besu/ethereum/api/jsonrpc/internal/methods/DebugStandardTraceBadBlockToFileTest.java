/*
 * Copyright ConsenSys AG.
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DebugStandardTraceBadBlockToFileTest {

  @TempDir private static Path folder;

  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final MutableWorldState mutableWorldState = mock(MutableWorldState.class);

  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final TransactionTracer transactionTracer = mock(TransactionTracer.class);

  private final BadBlockManager badBlockManager = new BadBlockManager();

  private final DebugStandardTraceBadBlockToFile debugStandardTraceBadBlockToFile =
      new DebugStandardTraceBadBlockToFile(
          () -> transactionTracer, blockchainQueries, protocolContext, folder);

  @BeforeEach
  public void setup() {
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    doAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<? extends JsonRpcResponse>>>getArgument(
                        1)
                    .apply(mutableWorldState))
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());
  }

  @Test
  public void nameShouldBeDebugTraceTransaction() {
    assertThat(debugStandardTraceBadBlockToFile.getName())
        .isEqualTo("debug_standardTraceBadBlockToFile");
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldTraceTheTransactionUsingTheTransactionTracer() {

    final BlockDataGenerator blockGenerator = new BlockDataGenerator();
    final Block genesis = blockGenerator.genesisBlock();
    final Block block =
        blockGenerator.block(
            new BlockDataGenerator.BlockOptions().setParentHash(genesis.getHeader().getHash()));

    final Object[] params = new Object[] {block.getHash(), new HashMap<>()};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "debug_standardTraceBadBlockToFile", params));

    final List<String> paths = new ArrayList<>();
    paths.add("path-1");

    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(transactionTracer.traceTransactionToFile(
            any(MutableWorldState.class), eq(block.getHash()), any(), any()))
        .thenReturn(paths);
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugStandardTraceBadBlockToFile.response(request);
    final List result = (ArrayList) response.getResult();

    assertThat(result.size()).isEqualTo(1);
  }
}
