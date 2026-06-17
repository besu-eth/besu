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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.eth.sync.common.BackwardHeaderDriver;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test: snap-sync chain download with anchor recovery after a deep reorg.
 *
 * <p>Unlike unit tests that mock {@link MutableBlockchain}, this test uses a real {@code
 * DefaultBlockchain} and drives {@link BackwardHeaderDriver} directly to verify that:
 *
 * <ol>
 *   <li>Stage 1 detects the anchor mismatch when the new canonical chain diverges below the
 *       previously stored anchor.
 *   <li>The recovery walk correctly finds the last common ancestor.
 *   <li>The blockchain's canonical header index is updated to reflect the new chain.
 *   <li>A simulated Stage 2 (bodies imported directly) advances the chain head to the pivot.
 * </ol>
 *
 * <p>Chain topology:
 *
 * <pre>
 * Genesis(0) → A1 → ... → A30 → A31 → ... → A50   ← old canonical, pre-loaded; anchor = A50
 *                             ↘ B31 → ... → B100   ← new canonical, pivot = B100
 * </pre>
 *
 * <p>Divergence at block 31 — blocks 0–30 are identical on both chains. The recovery walk must find
 * A30 (= B30) as the last common ancestor.
 */
class SnapSyncChainDownloadIntegrationTest {

  /** Number of the last block shared by both chains (the "fork parent"). */
  private static final int DIVERGENCE = 30;

  /** Block number used as the anchor in the persisted state from the old sync cycle. */
  private static final int OLD_ANCHOR = 50;

  /** Block number of the new canonical chain's pivot. */
  private static final int NEW_PIVOT = 100;

  /**
   * Batch size used for header download. Kept small so the recovery walk visits several batches and
   * the common-ancestor match happens at the fork block, not at genesis.
   */
  private static final int BATCH_SIZE = 4;

  /** A-chain: index i == block number i (genesis=0, A1=1, …, A50=50). */
  private static List<Block> aBlocks;

  /**
   * B-chain: index i == block number i (shared prefix genesis..A30, then B31..B100). Full {@link
   * Block} objects so Stage-2 simulation can access bodies.
   */
  private static List<Block> bBlocks;

  @BeforeAll
  static void buildChains() {
    // A-chain: genesis + 50 blocks
    final BlockDataGenerator genA = new BlockDataGenerator(1);
    aBlocks = genA.blockSequence(OLD_ANCHOR + 1); // indices 0..50

    // B-chain: reuse the shared prefix (indices 0..DIVERGENCE) then diverge
    final BlockDataGenerator genB = new BlockDataGenerator(2); // different seed → different hashes
    final List<Block> bExtension =
        genB.blockSequence(aBlocks.get(DIVERGENCE), NEW_PIVOT - DIVERGENCE); // B31..B100

    bBlocks = new ArrayList<>(NEW_PIVOT + 1);
    for (int i = 0; i <= DIVERGENCE; i++) {
      bBlocks.add(aBlocks.get(i)); // shared prefix: genesis..A30 (= B30)
    }
    bBlocks.addAll(bExtension); // diverging suffix: B31..B100

    // Sanity: B31's parent hash must equal B30's (= A30's) hash
    assertThat(bBlocks.get(DIVERGENCE + 1).getHeader().getParentHash())
        .isEqualTo(bBlocks.get(DIVERGENCE).getHeader().getHash());
  }

  // ── helpers ─────────────────────────────────────────────────────────────────────────────────

  /**
   * Simulates what {@link
   * org.hyperledger.besu.ethereum.eth.sync.common.DownloadBackwardHeadersStep} does: fetch a batch
   * of {@code BATCH_SIZE} headers from the B-chain starting at {@code topBlock} downward, capped at
   * the anchor in normal mode and uncapped (but limited by block 0) in recovery mode.
   */
  private static List<BlockHeader> serveBatch(final long topBlock, final long anchorNumber) {
    final long remaining =
        topBlock <= anchorNumber
            ? topBlock // recovery mode: remainingHeaders = startBlockNumber
            : topBlock - anchorNumber; // normal mode: remainingHeaders = startBlock - anchor
    final int headersToRequest = (int) Math.min(BATCH_SIZE, remaining);

    final List<BlockHeader> batch = new ArrayList<>();
    for (long n = topBlock; n > topBlock - headersToRequest && n >= 0; n--) {
      batch.add(bBlocks.get((int) n).getHeader());
    }
    return batch;
  }

  /**
   * Drives the source-thread loop on a single thread (safe because decisions is a BlockingQueue).
   */
  private static void driveToCompletion(
      final BackwardHeaderDriver driver, final long anchorNumber) {
    while (driver.hasNext()) {
      final long topBlock = driver.next();
      driver.accept(serveBatch(topBlock, anchorNumber));
    }
  }

  // ── test ────────────────────────────────────────────────────────────────────────────────────

  @Test
  void stageOneRecoversToForkParentAndStageTwoCompletesToPivot() {
    final ProtocolSchedule protocolSchedule = ProtocolScheduleFixture.TESTING_NETWORK;

    // ── Stage 1 setup ─────────────────────────────────────────────────────────────────────────

    // Blockchain pre-loaded with the old canonical A-chain:
    //   - genesis body (from createInMemoryBlockchain — establishes td=0 anchor)
    //   - A1..A50 headers (canonical number→hash index)
    //   - A1..A30 bodies imported via unsafeImportSyncBodiesAndReceipts to build the total-
    //     difficulty chain through the shared prefix. Without this, Stage 2 cannot compute
    //     the td for B31 because its parent (B30 = A30) has no stored td.
    final MutableBlockchain blockchain = createInMemoryBlockchain(aBlocks.get(0));
    blockchain.storeBlockHeaders(
        aBlocks.subList(1, OLD_ANCHOR + 1).stream().map(Block::getHeader).toList());

    final List<SyncBlockWithReceipts> sharedPrefixBodies =
        aBlocks.subList(1, DIVERGENCE + 1).stream()
            .map(
                block ->
                    new SyncBlockWithReceipts(
                        new SyncBlock(
                            block.getHeader(),
                            SyncBlockBody.emptyWithNullWithdrawals(protocolSchedule)),
                        Collections.emptyList()))
            .toList();
    blockchain.unsafeImportSyncBodiesAndReceipts(sharedPrefixBodies, false);

    final BlockHeader anchorHeader = aBlocks.get(OLD_ANCHOR).getHeader(); // A50
    final BlockHeader pivotHeader = bBlocks.get(NEW_PIVOT).getHeader(); // B100

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE, anchorHeader, pivotHeader, aBlocks.get(0).getHeader(), blockchain);

    // ── Drive Stage 1 ─────────────────────────────────────────────────────────────────────────
    driveToCompletion(driver, anchorHeader.getNumber());

    // ── Stage 1 assertions ────────────────────────────────────────────────────────────────────

    // Recovery must find A30 (= B30) as the last common ancestor.
    // The last batch to contain a diverging block is [B34..B31]; B31.parentHash = B30.hash =
    // A30.hash.
    // potentialParent = blockchain.getBlockHeader(30) = A30. Match → matchedAncestor = A30.
    assertThat(driver.getMatchedAncestor())
        .as("recovery must identify the fork parent (block %d) as the matched ancestor", DIVERGENCE)
        .isPresent()
        .hasValueSatisfying(
            matched -> assertThat(matched.getNumber()).isEqualTo((long) DIVERGENCE));

    // The canonical header index for the diverged range must now reflect the B-chain.
    assertThat(blockchain.getBlockHashByNumber((long) DIVERGENCE + 1))
        .as("canonical hash at first diverged block should be B31")
        .contains(bBlocks.get(DIVERGENCE + 1).getHeader().getHash());

    assertThat(blockchain.getBlockHashByNumber((long) NEW_PIVOT))
        .as("canonical hash at pivot should be B100")
        .contains(pivotHeader.getHash());

    // Blocks at or below the fork parent must still carry the shared (= A-chain = B-chain) hash.
    assertThat(blockchain.getBlockHashByNumber((long) DIVERGENCE))
        .as("shared fork-parent block should be unchanged")
        .contains(aBlocks.get(DIVERGENCE).getHeader().getHash());

    // ── Stage 2 simulation ────────────────────────────────────────────────────────────────────
    // At this point the blockchain has all B-chain headers (0..100). Simulates what Stage 2
    // does: import block bodies from the matched ancestor+1 up to the pivot. Bodies are empty
    // (the unsafe import bypasses transactionsRoot validation, matching snap sync behaviour).

    final List<SyncBlockWithReceipts> bodies =
        IntStream.rangeClosed(DIVERGENCE + 1, NEW_PIVOT)
            .mapToObj(
                i ->
                    new SyncBlockWithReceipts(
                        new SyncBlock(
                            bBlocks.get(i).getHeader(),
                            SyncBlockBody.emptyWithNullWithdrawals(protocolSchedule)),
                        Collections.emptyList()))
            .toList();

    blockchain.unsafeImportSyncBodiesAndReceipts(bodies, false);

    // ── Stage 2 assertions ────────────────────────────────────────────────────────────────────

    assertThat(blockchain.getBlockBody(pivotHeader.getHash()))
        .as("pivot block B100 must have a body after Stage 2")
        .isPresent();

    assertThat(blockchain.getBlockHashByNumber((long) NEW_PIVOT))
        .as("canonical hash at pivot must still be B100 after Stage 2")
        .contains(pivotHeader.getHash());
  }
}
