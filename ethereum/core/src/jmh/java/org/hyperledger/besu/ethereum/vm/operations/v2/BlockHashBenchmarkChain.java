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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.vm.BlockchainBasedBlockHashLookup;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Builds an in-memory header chain and a {@link BlockchainBasedBlockHashLookup} for BLOCKHASH JMH
 * fixtures. Parent-hash lookups hit the lookup cache; deeper hashes walk headers via the mocked
 * {@link Blockchain}.
 */
final class BlockHashBenchmarkChain {

  static final long CURRENT_BLOCK = 1_000;
  static final long VALID_BLOCK = CURRENT_BLOCK - 1;

  private final BlockHeader currentHeader;
  private final BlockHashLookup blockHashLookup;

  private BlockHashBenchmarkChain(
      final BlockHeader currentHeader, final BlockHashLookup blockHashLookup) {
    this.currentHeader = currentHeader;
    this.blockHashLookup = blockHashLookup;
  }

  static BlockHashBenchmarkChain create() {
    final Map<Hash, BlockHeader> headersByHash = new HashMap<>();
    BlockHeader parentHeader = null;
    for (int i = 0; i < CURRENT_BLOCK; i++) {
      final BlockHeader header = createHeader(i, parentHeader);
      headersByHash.put(header.getHash(), header);
      parentHeader = header;
    }

    final Blockchain blockchain = mock(Blockchain.class);
    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(
            invocation -> Optional.ofNullable(headersByHash.get(invocation.getArgument(0))));

    final BlockHeader currentHeader = createHeader((int) CURRENT_BLOCK, parentHeader);
    final BlockHashLookup blockHashLookup =
        new BlockchainBasedBlockHashLookup(currentHeader, blockchain);
    return new BlockHashBenchmarkChain(currentHeader, blockHashLookup);
  }

  BlockHeader currentHeader() {
    return currentHeader;
  }

  BlockHashLookup blockHashLookup() {
    return blockHashLookup;
  }

  private static BlockHeader createHeader(final int blockNumber, final BlockHeader parentHeader) {
    return new BlockHeaderTestFixture()
        .number(blockNumber)
        .parentHash(parentHeader != null ? parentHeader.getHash() : Hash.EMPTY)
        .buildHeader();
  }
}
