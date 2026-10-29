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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archives a range by rolling a layered copy of the head world state back to the end of coverage,
 * then forward one block at a time with an archiver strategy installed on the layer. The writer
 * commits each block's history and coverage on its own; the layer's writes are discarded.
 */
public final class RollingTrieNodeArchiveStep implements TrieNodeArchiveStep {

  private static final Logger LOG = LoggerFactory.getLogger(RollingTrieNodeArchiveStep.class);

  /** Rolls {@code state} to {@code blockHash}, throwing if it cannot. */
  @FunctionalInterface
  public interface StateRoller {
    void rollTo(PathBasedWorldState state, Hash blockHash);
  }

  private final Supplier<PathBasedWorldState> headStateOpener;
  private final StateRoller roller;
  private final Blockchain blockchain;
  private final TrieLogManager trieLogManager;
  private final ArchiveCoverageTracker coverage;
  private final Supplier<ArchiveTrieNodeWriter> writerFactory;

  public RollingTrieNodeArchiveStep(
      final Supplier<PathBasedWorldState> headStateOpener,
      final StateRoller roller,
      final Blockchain blockchain,
      final TrieLogManager trieLogManager,
      final ArchiveCoverageTracker coverage,
      final Supplier<ArchiveTrieNodeWriter> writerFactory) {
    this.headStateOpener = headStateOpener;
    this.roller = roller;
    this.blockchain = blockchain;
    this.trieLogManager = trieLogManager;
    this.coverage = coverage;
    this.writerFactory = writerFactory;
  }

  @Override
  public void archiveRange(
      final long fromExclusive, final long toInclusive, final BooleanSupplier keepGoing) {
    if (toInclusive <= fromExclusive) {
      return;
    }
    try (final PathBasedWorldState state = headStateOpener.get()) {
      roller.rollTo(state, canonicalHash(fromExclusive));
      state
          .getWorldStateStorage()
          .setTrieNodeStrategy(
              new ArchiveTrieNodeStrategy(
                  new BonsaiTrieNodeStrategy(),
                  writerFactory.get(),
                  coverage,
                  storage -> true,
                  block -> true));
      for (long block = fromExclusive + 1;
          block <= toInclusive && keepGoing.getAsBoolean();
          block++) {
        if (!archiveBlock(state, block)) {
          return;
        }
      }
    }
  }

  private boolean archiveBlock(final PathBasedWorldState state, final long block) {
    final Hash blockHash = canonicalHash(block);
    if (trieLogManager.getTrieLogLayer(blockHash).isEmpty()) {
      throw new ArchiveGapException("the trie log for block " + block + " is missing");
    }
    roller.rollTo(state, blockHash);
    if (coverage.getCoverage().map(Coverage::lastBlock).orElse(-1L) < block) {
      LOG.warn("Trie-node archiver did not capture block {}; stopping this range", block);
      return false;
    }
    return true;
  }

  private Hash canonicalHash(final long block) {
    return blockchain
        .getBlockHeader(block)
        .map(BlockHeader::getHash)
        .orElseThrow(() -> new IllegalStateException("No canonical header for block " + block));
  }
}
