/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link BackwardHeaderDriver} recovery using a real in-memory blockchain.
 *
 * <p>Unlike the unit tests in {@code BackwardHeaderDriverTest} that mock {@code MutableBlockchain},
 * these tests use a real {@code DefaultBlockchain} and verify that:
 *
 * <ul>
 *   <li>{@code storeBlockHeaders} correctly updates the canonical {@code (number → hash)} index as
 *       the driver imports batches.
 *   <li>The recovery walk's {@code getBlockHeader(n)} reads from that live index and correctly
 *       identifies the shared ancestor between the old and new canonical chains.
 * </ul>
 *
 * <p>Chain topology used in every test:
 *
 * <pre>
 * Genesis → A1 → A2 → A3 → A4 → A5 → A6 → A7 → A8 → A9 → A10   (old canonical, pre-loaded)
 *                                  ↘ B6 → B7 → B8 → B9 → B10 → B11 → B12 → B13 → B14 → B15
 *                                                                                 (new canonical)
 * </pre>
 *
 * A-chain and B-chain share genesis through A5. B-chain diverges at block 6.
 */
public class BackwardHeaderDriverRecoveryIntegrationTest {

  private static final int BATCH_SIZE = 4;
  private static final LabelledMetric<Counter> RECOVERY_COUNTER =
      new NoOpMetricsSystem()
          .createLabelledCounter(
              BesuMetricCategory.SYNCHRONIZER,
              "test_recovery_counter",
              "test recovery counter",
              "result");

  /** A-chain: index i == block number i (genesis=0, A1=1, ..., A10=10). */
  private static List<Block> aChain;

  /** B-chain: index i == block number i (genesis=0, A1=1, ..., A5=5, B6=6, ..., B15=15). */
  private static List<BlockHeader> bChain;

  @BeforeAll
  public static void buildChains() {
    final BlockDataGenerator genA = new BlockDataGenerator(1);
    aChain = genA.blockSequence(11); // genesis(0) + A1..A10

    // B-chain: shares genesis..A5 with A-chain, diverges at block 6.
    final BlockDataGenerator genB = new BlockDataGenerator(2); // different seed → different hashes
    final List<Block> bBlocks = genB.blockSequence(aChain.get(5), 10); // B6..B15 (10 blocks)

    bChain = new ArrayList<>(16);
    for (int i = 0; i <= 5; i++) {
      bChain.add(aChain.get(i).getHeader()); // shared prefix: genesis..A5
    }
    for (Block b : bBlocks) {
      bChain.add(b.getHeader()); // diverging suffix: B6..B15
    }
    // sanity: B6.parentHash must equal A5.hash
    assert bChain.get(6).getParentHash().equals(aChain.get(5).getHeader().getHash())
        : "B6 must descend from A5";
  }

  /**
   * Simulates what the download pipeline does: fetches {@code batchSize} headers from {@code
   * topBlock} downward, clamped at {@code lowestHeaderToImport} in normal mode (the pipeline does
   * not request headers below the anchor boundary before recovery extends it).
   */
  private static List<BlockHeader> batchFrom(
      final long topBlock, final int batchSize, final long lowestHeaderToImport) {
    final long bottom =
        topBlock < lowestHeaderToImport
            // Recovery mode: no anchor cap, full batch (driver already extended the range)
            ? topBlock - batchSize + 1
            // Normal mode: clamp at the original anchor boundary
            : Math.max(topBlock - batchSize + 1, lowestHeaderToImport);
    final List<BlockHeader> batch = new ArrayList<>();
    for (long n = topBlock; n >= Math.max(bottom, 0); n--) {
      batch.add(bChain.get((int) n));
    }
    return batch;
  }

  /**
   * Drives the iterator + consumer loop on a single thread. Safe because {@code decisions} is a
   * {@link java.util.concurrent.BlockingQueue}: {@code accept()} adds a decision before {@code
   * hasNext()} ever blocks on {@code take()}.
   */
  private static void driveToCompletion(
      final BackwardHeaderDriver driver, final long lowestHeaderToImport) {
    while (driver.hasNext()) {
      final long top = driver.next();
      driver.accept(batchFrom(top, BATCH_SIZE, lowestHeaderToImport));
    }
  }

  // ---- tests ---------------------------------------------------------------------------------

  @Test
  void recoveryWalksBackToSharedAncestorWhenPivotChainDiverges() {
    // Anchor = A10 (previous pivot, now on side-chain).
    // New pivot = B15 (on the diverged canonical chain).
    // Recovery must walk below block 11 until it finds a header already stored in the blockchain.
    final MutableBlockchain blockchain = createInMemoryBlockchain(aChain.get(0));
    blockchain.storeBlockHeaders(
        aChain.subList(1, 11).stream().map(Block::getHeader).toList()); // A1..A10

    final BlockHeader anchor = aChain.get(10).getHeader(); // A10
    final BlockHeader pivot = bChain.get(15); // B15

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchor, pivot, blockchain);

    final long lowestHeaderToImport = anchor.getNumber() + 1; // 11
    driveToCompletion(driver, lowestHeaderToImport);

    // Recovery should have matched A2: the lowest A-chain header whose parent (A1) is
    // stored and whose hash matches the B-chain header at block 3 (which IS A3, a shared block).
    assertThat(driver.getMatchedAncestor())
        .isPresent()
        .hasValueSatisfying(matched -> assertThat(matched.getNumber()).isEqualTo(2L));

    // The canonical index at block 6 must now reflect the B-chain (written during the walk).
    assertThat(blockchain.getBlockHashByNumber(6L)).contains(bChain.get(6).getHash());

    // Blocks at or below the shared ancestor (≤ block 2) must still carry A-chain hashes,
    // because the driver only overwrote canonical entries for blocks it actually stored.
    assertThat(blockchain.getBlockHashByNumber(2L)).contains(aChain.get(2).getHeader().getHash());
    assertThat(blockchain.getBlockHashByNumber(1L)).contains(aChain.get(1).getHeader().getHash());
  }

  @Test
  void noRecoveryWhenAnchorIsOnSharedPrefix() {
    // Anchor = A5 (below the divergence point — B6.parentHash == A5.hash).
    // The boundary batch contains exactly [B6] (clamped at lowestHeaderToImport = 6).
    // B6.parentHash == A5.hash == anchorHash → happy path, no recovery.
    final MutableBlockchain blockchain = createInMemoryBlockchain(aChain.get(0));
    blockchain.storeBlockHeaders(
        aChain.subList(1, 6).stream().map(Block::getHeader).toList()); // A1..A5

    final BlockHeader anchor = aChain.get(5).getHeader(); // A5
    final BlockHeader pivot = bChain.get(15); // B15

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchor, pivot, blockchain);

    final long lowestHeaderToImport = anchor.getNumber() + 1; // 6
    driveToCompletion(driver, lowestHeaderToImport);

    assertThat(driver.getMatchedAncestor()).isEmpty();

    // The canonical index should have been updated all the way to block 6 (B6).
    assertThat(blockchain.getBlockHashByNumber(6L)).contains(bChain.get(6).getHash());
  }
}
