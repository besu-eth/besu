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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;

import org.junit.jupiter.api.Test;

public class BlockDataBoundaryTest {

  private BlockchainStorage storage() {
    return new KeyValueStoragePrefixedKeyBlockchainStorage(
        new InMemoryKeyValueStorage(),
        new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
        new MainnetBlockHeaderFunctions(),
        false);
  }

  @Test
  public void intactChainReportsGenesis() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage storage = storage();
    final Block genesis = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(genesis, storage, new NoOpMetricsSystem(), 0);
    gen.blockSequence(genesis, 20).forEach(b -> blockchain.appendBlock(b, gen.receipts(b)));

    assertThat(BlockDataBoundary.firstBlockWithBody(BlockDataBoundary.blockDataIn(storage), 20))
        .contains(0L);
  }

  @Test
  public void reportsFirstBlockThatStillHasABody() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage storage = storage();
    final Block genesis = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(genesis, storage, new NoOpMetricsSystem(), 0);
    final List<Block> blocks = gen.blockSequence(genesis, 20);
    blocks.forEach(b -> blockchain.appendBlock(b, gen.receipts(b)));

    // Remove the bodies of blocks 1-5, leaving headers in place, as chain pruning does.
    final BlockchainStorage.Updater updater = storage.updater();
    for (int i = 0; i < 5; i++) {
      updater.removeBlockBody(blocks.get(i).getHash());
    }
    updater.commit();

    assertThat(BlockDataBoundary.firstBlockWithBody(BlockDataBoundary.blockDataIn(storage), 20))
        .contains(6L);
  }
}
