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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockImporter;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adaptor class to allow a {@link BlockImporter} to be used as a {@link QbftBlockImporter}. */
public class QbftBlockImporterAdaptor implements QbftBlockImporter {

  private static final Logger LOG = LoggerFactory.getLogger(QbftBlockImporterAdaptor.class);

  private final BlockImporter blockImporter;
  private final BlockHeaderValidator blockHeaderValidator;
  private final ProtocolContext context;
  private final ValidatedBlockCache validatedBlockCache;

  /**
   * Constructs a new Qbft block importer.
   *
   * @param blockImporter The Besu block importer
   * @param blockHeaderValidator The header validator applied to blocks imported from the cache
   * @param context The protocol context
   * @param validatedBlockCache outputs of created and validated blocks
   */
  public QbftBlockImporterAdaptor(
      final BlockImporter blockImporter,
      final BlockHeaderValidator blockHeaderValidator,
      final ProtocolContext context,
      final ValidatedBlockCache validatedBlockCache) {
    this.blockImporter = blockImporter;
    this.blockHeaderValidator = blockHeaderValidator;
    this.context = context;
    this.validatedBlockCache = validatedBlockCache;
  }

  @Override
  public boolean importBlock(
      final QbftBlock block, final Optional<BlockAccessList> blockAccessList) {
    final Block besuBlock = AdaptorUtil.toBesuBlock(block);
    final Optional<ValidatedBlockCache.ValidatedBlock> validatedBlock =
        validatedBlockCache.take(besuBlock.getHash(), besuBlock.getHeader().getNumber());
    if (validatedBlock.isPresent() && importValidatedBlock(besuBlock, validatedBlock.get())) {
      return true;
    }
    final BlockImportResult blockImportResult =
        blockImporter.importBlock(
            context,
            besuBlock,
            HeaderValidationMode.FULL,
            HeaderValidationMode.FULL,
            blockAccessList);
    return blockImportResult.isImported();
  }

  /**
   * Imports a block whose proposal was already processed, without executing it again. Block
   * creation or validation saved its trie log, used to move the head world state after append, like
   * {@link org.hyperledger.besu.ethereum.mainnet.MainnetBlockImporter}.
   *
   * <p>The proposal hash only matches the hash of the committed block in round 0, where the round
   * number is zero in both. A block committed in a later round is not found in the cache and is
   * processed again.
   *
   * @return false when the block must go through the regular import instead
   */
  private boolean importValidatedBlock(
      final Block block, final ValidatedBlockCache.ValidatedBlock validatedBlock) {
    final MutableBlockchain blockchain = context.getBlockchain();
    final BlockHeader header = block.getHeader();
    // MainnetBlockImporter.importBlock is synchronized on the importer, which is shared with full
    // sync. Take the same lock so the head check, append and world state move are not interleaved
    // with another import. The adaptor itself is created per import, so it cannot be the lock.
    synchronized (blockImporter) {
      if (!header.getParentHash().equals(blockchain.getChainHeadHash())
          || !hasTrieLog(context.getWorldStateArchive(), header)) {
        return false;
      }
      final Optional<BlockHeader> parentHeader = blockchain.getBlockHeader(header.getParentHash());
      if (parentHeader.isEmpty()
          || !blockHeaderValidator.validateHeader(
              header, parentHeader.get(), context, HeaderValidationMode.FULL)) {
        return false;
      }

      blockchain.appendBlock(block, validatedBlock.receipts(), validatedBlock.blockAccessList());
      if (context
          .getWorldStateArchive()
          .getWorldState(withBlockHeaderAndUpdateNodeHead(header))
          .isEmpty()) {
        LOG.warn("Unable to move the head world state to imported block {}", block.toLogString());
      }
    }
    LOG.debug("Imported block {} without executing it again", block.toLogString());
    return true;
  }

  private static boolean hasTrieLog(
      final WorldStateArchive worldStateArchive, final BlockHeader header) {
    return worldStateArchive instanceof PathBasedWorldStateProvider provider
        && provider.getWorldStateKeyValueStorage().getTrieLog(header.getHash()).isPresent();
  }
}
