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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.util.RawBlockIterator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** Creates a block chain from a genesis and a blocks files. */
public class BlockchainImporter {

  private final GenesisState genesisState;

  private final ProtocolSchedule protocolSchedule;

  private final List<Block> blocks;

  private final Block genesisBlock;

  public BlockchainImporter(final URL blocksUrl, final String genesisJson) throws Exception {
    protocolSchedule =
        MainnetProtocolSchedule.fromConfig(
            GenesisConfig.fromConfig(genesisJson).getConfigOptions(),
            MiningConfiguration.newDefault(),
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final BlockHeaderFunctions blockHeaderFunctions =
        ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
    blocks = new ArrayList<>();
    try (final RawBlockIterator iterator =
        new RawBlockIterator(Paths.get(blocksUrl.toURI()), blockHeaderFunctions)) {
      while (iterator.hasNext()) {
        blocks.add(iterator.next());
      }
    }

    genesisBlock = blocks.get(0);
    // only used in tests no global code cache is needed
    genesisState = GenesisState.fromJson(genesisJson, protocolSchedule, new PathBasedCodeCache());
  }

  public GenesisState getGenesisState() {
    return genesisState;
  }

  public ProtocolSchedule getProtocolSchedule() {
    return protocolSchedule;
  }

  public List<Block> getBlocks() {
    return blocks;
  }

  public Block getGenesisBlock() {
    return genesisBlock;
  }
}
