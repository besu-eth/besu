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
package org.hyperledger.besu.tests.acceptance.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.snapsync.AmsterdamEngineApi.BuiltBlock;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.web3j.crypto.RawTransaction;

/**
 * Snap/2 reorg recovery, end to end: a node snap-syncs toward fork A, and while its world-state
 * download is still running its only peer reorgs onto a longer fork B. The reorg healer must repair
 * the partially downloaded state; the node must finish on fork B, and the healed state must be able
 * to execute new blocks.
 *
 * <p>The forks share only block 1 and are arranged to hit every reorg category: contracts touched
 * on both forks (the canonical BAL wins), slots and contracts that existed only on the orphaned
 * fork (re-fetched, then cleared), an account created only on the canonical fork (added), and
 * untouched state (left alone). See {@link #assertHealedWorldState()}.
 *
 * <p>Flaky-test note: recovery only fires if the pivot switches to fork B while fork A's
 * world-state download is still running. The 2000 deployed contracts plus the throttled downloader
 * keep that window open for several seconds; if this starts timing out on a slow CI agent, retune
 * {@link #CONTRACTS_PER_HEAVY_BLOCK}.
 */
public class SnapV2ReorgRecoveryAcceptanceTest extends AbstractSnapV2AcceptanceTest {

  // Fork geometry: PivotSelectorAtHead anchors the pivot at head - 1, so pivotA = 73 and
  // pivotB = 84. Distinct fee recipients diverge the forks at block 2; all scenario transactions
  // are in blocks 2-3, below both pivots.
  private static final int COMMON_HEIGHT = 1;
  private static final int FORK_A_HEIGHT = 74;
  private static final int FORK_B_HEIGHT = 85;

  // Phase 1 (head=74, pivot=73): lag=1 < PIVOT_BLOCK_WINDOW_VALIDITY, pivot reused. Phase 2
  // (head=85): lag=12 >= 10, pivot refreshes to fork B's pivot, triggering the continuation round
  // and the reorg healer. FORK_B_HEIGHT must keep the ancestor walk from pivotB to the shared
  // block 1 under SnapV2ReorgHealer.MAX_ANCESTOR_WALK (95).
  private static final int HEAVY_BLOCK_1 = 2;
  private static final int HEAVY_BLOCK_2 = 3;

  // Blocks 2-3 of each fork deploy 1000 contracts each, so the throttled world-state download
  // runs for several seconds: the window in which the pivot is switched to fork B.
  private static final int CONTRACTS_PER_HEAVY_BLOCK = 1000;
  private static final int STORAGE_SLOTS_PER_CONTRACT = 2;
  private static final int SPLIT_SLOTS_FORK_A = 24;
  private static final int SPLIT_SLOTS_FORK_B = 12;

  // Slot values written by the scenario contracts, per fork, so the winning fork is visible.
  private static final int FORK_A_VALUE = 1;
  private static final int FORK_B_VALUE = 2;

  // Same benefactor nonces on both forks => same contract addresses on both forks, so the same
  // state is touched with different values. Nonce 2000 is the split-slot contract, 2001-2005 the
  // orphaned-only contracts, 2006 the fork-A-only transfer (fork B stops after its own transfer
  // at 2001). 2007 is the post-sync transfer, above anything the reorg can re-insert.
  private static final long SPLIT_CONTRACT_NONCE = 2 * CONTRACTS_PER_HEAVY_BLOCK;
  private static final long ORPHAN_CONTRACT_NONCE_START = SPLIT_CONTRACT_NONCE + 1;
  private static final int ORPHAN_CONTRACT_COUNT = 5;
  private static final long POST_SYNC_TRANSFER_NONCE =
      ORPHAN_CONTRACT_NONCE_START + ORPHAN_CONTRACT_COUNT + 1;

  // Fresh addresses that no genesis alloc or deployed contract can collide with.
  private static final String FORK_A_ONLY_RECIPIENT = "0x1000000000000000000000000000000000000001";
  private static final String FORK_B_ONLY_RECIPIENT = "0x1000000000000000000000000000000000000002";

  private static final String FEE_RECIPIENT_A = "0x1111111111111111111111111111111111111111";
  private static final String FEE_RECIPIENT_B = "0x2222222222222222222222222222222222222222";

  private BesuNode minerA;
  private BesuNode minerB;

  // Benefactor nonces per fork (they diverge after the split-slot contract at nonce 2000).
  private long nonceForkA = 0;
  private long nonceForkB = 0;

  @Test
  public void recoversFromReorgPastPivotDuringSnapV2Sync() throws Exception {
    startNodes();
    buildSharedBlockOne();

    final List<BuiltBlock> forkA =
        buildFork(minerA, FEE_RECIPIENT_A, FORK_A_HEIGHT, this::submitForkATxs);
    final List<BuiltBlock> forkB =
        buildFork(minerB, FEE_RECIPIENT_B, FORK_B_HEIGHT, this::submitForkBTxs);
    final BuiltBlock forkAHead = forkA.getLast();
    final String forkBHeadHash = forkB.getLast().blockHash();

    // Pre-cache fork B's headers on the sync node so its pivot selector can resolve fork B's
    // pivot from cache. Inert until the forkchoice points at fork B.
    for (final BuiltBlock block : forkB) {
      engineApi.cachePayload(syncNode, block);
    }

    // Capture from here on: the log lines we key off are emitted by the sync node.
    noDiscoveryCluster.startConsoleCapture();

    // Phase 1: snap-sync toward fork A, connected only to miner A.
    syncNode.execute(adminTransactions.addPeer(minerA.enodeUrl()));
    syncNode.verify(net.awaitPeerCount(1));
    engineApi.cachePayload(syncNode, forkAHead);
    awaitLog(
        "Header import progress 100.00%",
        Duration.ofMinutes(3), Duration.ofMillis(50), forkAHead.blockHash());

    // The reorg healer reads orphaned-fork BALs from local storage, so fork A's BALs must be
    // persisted locally before the reorg is triggered.
    awaitOrphanedBalsPersisted();

    // Phase 2: miner A pulls fork B from miner B and reorgs onto its higher head, so the sync
    // node's single peer flips forks mid-download. The frequently re-checked pivot advances to
    // fork B, and the pivot catch-up detects the old pivot is no longer canonical.
    minerA.execute(adminTransactions.addPeer(minerB.enodeUrl()));
    engineApi.setHead(minerA, forkBHeadHash);
    engineApi.setHead(syncNode, forkBHeadHash);
    awaitLog(
        "snap/2 chain reorg detected at pivot catch-up",
        Duration.ofMinutes(4),
        Duration.ofMillis(250),
        () -> {
          engineApi.setHead(minerA, forkBHeadHash);
          engineApi.setHead(syncNode, forkBHeadHash);
        });

    // The sync node fully adopts fork B once the healer has corrected the partially downloaded
    // world state and the remaining ranges are downloaded at the fork-B pivot.
    awaitHead(forkBHeadHash, FORK_B_HEIGHT);

    final String syncConsole = noDiscoveryCluster.peekConsoleContents();
    assertThat(syncConsole).contains("snap/2 reorg recovery complete");
    assertThat(syncConsole).doesNotContain("snap/2 pivot catch-up failed");
    assertThat(blockAt(syncNode, FORK_B_HEIGHT).getHash()).isEqualTo(forkBHeadHash);

    assertHealedWorldState();
    assertHealedStateExecutesNewBlocks();
  }

  /** State assertions on the healed world state, one per reorg category. */
  private void assertHealedWorldState() {
    // Touched on both forks: the canonical BAL wins, so the slots read fork B's values.
    final Account heavyContract0 = contractAt(0);
    assertStorage(heavyContract0, 0, FORK_B_VALUE);
    assertStorage(heavyContract0, 1, FORK_B_VALUE);

    // Slot split across forks (24 on fork A, 12 on fork B): an overlapping slot reads fork B's
    // value; the first orphaned-only slot was re-fetched from the canonical chain and cleared.
    final Account splitContract = contractAt(SPLIT_CONTRACT_NONCE);
    assertStorage(splitContract, 0, FORK_B_VALUE);
    assertStorage(splitContract, SPLIT_SLOTS_FORK_B, 0);

    // Orphaned fork only: the contract and the transfer recipient are gone entirely.
    assertNoContract(contractAt(ORPHAN_CONTRACT_NONCE_START));
    assertBalance(FORK_A_ONLY_RECIPIENT, BigInteger.ZERO);

    // Canonical fork only: the recipient created by fork B's BAL exists with its balance.
    assertBalance(FORK_B_ONLY_RECIPIENT, TRANSFER_WEI);
  }

  /**
   * A fresh block built on fork B's head must validate on the sync node: the healed world state can
   * execute new blocks. Its transfer re-creates the fork-A-only recipient.
   */
  private void assertHealedStateExecutesNewBlocks() throws IOException {
    replaceReinsertedOrphanTxs();
    submitTransfer(minerB, POST_SYNC_TRANSFER_NONCE, FORK_A_ONLY_RECIPIENT);

    final BuiltBlock postSyncBlock =
        engineApi.buildBlock(minerB, FEE_RECIPIENT_B, ORPHAN_CONTRACT_COUNT + 1, FORK_B_HEIGHT + 1);
    engineApi.assertValidPayload(syncNode, postSyncBlock);
    engineApi.setHead(syncNode, postSyncBlock.blockHash());
    syncNode.verify(blockchain.currentHeight(FORK_B_HEIGHT + 1));
    assertBalance(FORK_A_ONLY_RECIPIENT, TRANSFER_WEI);
  }

  /**
   * The reorg re-inserted the orphaned fork-A transactions (nonces 2002-2006) into miner B's pool.
   * Outbid them with harmless self-transfers so the next block does not resurrect the contracts and
   * the transfer the healer deleted.
   */
  private void replaceReinsertedOrphanTxs() {
    for (long nonce = ORPHAN_CONTRACT_NONCE_START + 1;
        nonce <= ORPHAN_CONTRACT_NONCE_START + ORPHAN_CONTRACT_COUNT;
        nonce++) {
      sendRaw(
          minerB,
          RawTransaction.createEtherTransaction(
              BigInteger.valueOf(nonce),
              BigInteger.valueOf(2_000), // gas price outbids the re-inserted orphaned txs
              BigInteger.valueOf(21_000),
              BENEFACTOR_ADDRESS.toHexString(),
              BigInteger.ZERO));
    }
  }

  private void startNodes() throws IOException {
    final String genesis = loadAmsterdamGenesis();
    minerA = createMiner("minerA", genesis);
    minerB = createMiner("minerB", genesis);
    syncNode = createSyncNode("syncNode", genesis);
    startCluster(minerA, minerB, syncNode);
  }

  /** Builds block 1 on miner A and imports it into miner B, so both forks share it. */
  private void buildSharedBlockOne() throws IOException {
    engineApi.importBlock(minerB, engineApi.buildBlock(minerA, FEE_RECIPIENT_A, 0, COMMON_HEIGHT));
    minerA.verify(blockchain.currentHeight(COMMON_HEIGHT));
    minerB.verify(blockchain.currentHeight(COMMON_HEIGHT));
  }

  /**
   * Builds a fork chain on {@code miner} above the shared block. Fork A and fork B deploy the same
   * contracts (same benefactor nonces => same addresses) but write different slot values; fork A
   * adds orphaned-only contracts and a transfer, fork B its own transfer.
   */
  private List<BuiltBlock> buildFork(
      final BesuNode miner, final String feeRecipient, final int forkHeight, final TxSubmitter txs)
      throws IOException {
    final List<BuiltBlock> blocks = new ArrayList<>();
    for (int height = COMMON_HEIGHT + 1; height <= forkHeight; height++) {
      blocks.add(engineApi.buildBlock(miner, feeRecipient, txs.submit(miner, height), height));
    }
    miner.verify(blockchain.currentHeight(forkHeight));
    return blocks;
  }

  /** Submits the scenario transactions for one block; returns how many were submitted. */
  @FunctionalInterface
  private interface TxSubmitter {
    int submit(BesuNode miner, int height);
  }

  /**
   * Fork A's scenario: the same deploys as fork B but writing {@link #FORK_A_VALUE}, plus the
   * orphaned-only contracts and transfer. Nonces are consumed in submission order, so by the second
   * heavy block the counter has reached {@link #SPLIT_CONTRACT_NONCE} exactly.
   */
  private int submitForkATxs(final BesuNode miner, final int height) {
    final long nonceBefore = nonceForkA;
    if (height == HEAVY_BLOCK_1 || height == HEAVY_BLOCK_2) {
      for (int i = 0; i < CONTRACTS_PER_HEAVY_BLOCK; i++) {
        submitDeploy(
            miner, nonceForkA++, STORAGE_SLOTS_PER_CONTRACT, FORK_A_VALUE, DEPLOY_GAS_LIMIT);
      }
      if (height == HEAVY_BLOCK_2) {
        submitDeploy(miner, nonceForkA++, SPLIT_SLOTS_FORK_A, FORK_A_VALUE, SPLIT_DEPLOY_GAS_LIMIT);
        for (int i = 0; i < ORPHAN_CONTRACT_COUNT; i++) {
          submitDeploy(
              miner, nonceForkA++, STORAGE_SLOTS_PER_CONTRACT, FORK_A_VALUE, DEPLOY_GAS_LIMIT);
        }
        submitTransfer(miner, nonceForkA++, FORK_A_ONLY_RECIPIENT);
      }
    }
    // the submitted count is exactly the nonces consumed
    return (int) (nonceForkA - nonceBefore);
  }

  /** Fork B's scenario: fork A's deploys minus the orphaned-only state, writing FORK_B_VALUE. */
  private int submitForkBTxs(final BesuNode miner, final int height) {
    final long nonceBefore = nonceForkB;
    if (height == HEAVY_BLOCK_1 || height == HEAVY_BLOCK_2) {
      for (int i = 0; i < CONTRACTS_PER_HEAVY_BLOCK; i++) {
        submitDeploy(
            miner, nonceForkB++, STORAGE_SLOTS_PER_CONTRACT, FORK_B_VALUE, DEPLOY_GAS_LIMIT);
      }
      if (height == HEAVY_BLOCK_2) {
        submitDeploy(miner, nonceForkB++, SPLIT_SLOTS_FORK_B, FORK_B_VALUE, SPLIT_DEPLOY_GAS_LIMIT);
        submitTransfer(miner, nonceForkB++, FORK_B_ONLY_RECIPIENT);
      }
    }
    return (int) (nonceForkB - nonceBefore);
  }

  private void submitDeploy(
      final BesuNode miner,
      final long nonce,
      final int slotCount,
      final int slotValue,
      final long gasLimit) {
    sendRaw(
        miner,
        RawTransaction.createContractTransaction(
            BigInteger.valueOf(nonce),
            GAS_PRICE,
            BigInteger.valueOf(gasLimit),
            BigInteger.ZERO,
            storageContractInitCode(slotCount, slotValue)));
  }

  private void submitTransfer(final BesuNode miner, final long nonce, final String recipient) {
    sendRaw(
        miner,
        RawTransaction.createEtherTransaction(
            BigInteger.valueOf(nonce),
            GAS_PRICE,
            BigInteger.valueOf(TRANSFER_GAS_LIMIT),
            recipient,
            TRANSFER_WEI));
  }

  private void sendRaw(final BesuNode node, final RawTransaction tx) {
    sendRaw(node, tx, BENEFACTOR);
  }

  /**
   * EVM init code that writes {@code slotCount} slots (slot j = {@code slotValue}) in the
   * constructor, then returns a 1-byte runtime. Slot indices and values must fit in one byte (PUSH1
   * operands).
   */
  private static String storageContractInitCode(final int slotCount, final int slotValue) {
    final StringBuilder code = new StringBuilder("0x");
    for (int j = 0; j < slotCount; j++) {
      // PUSH1 value ; PUSH1 j ; SSTORE
      code.append("60")
          .append(String.format("%02x", slotValue))
          .append("60")
          .append(String.format("%02x", j))
          .append("55");
    }
    // PUSH1 0 ; PUSH1 0 ; MSTORE ; PUSH1 1 ; PUSH1 31 ; RETURN
    code.append("60006000526001601ff3");
    return code.toString();
  }

  /** Waits until fork A's BALs are persisted locally (block 2 is below the fork-A pivot). */
  private void awaitOrphanedBalsPersisted() {
    await()
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> engineApi.hasBlockAccessList(syncNode, "0x2"));
  }

  private void assertNoContract(final Account contract) {
    assertThat(syncNode.execute(ethTransactions.getCode(contract))).isEqualTo(Bytes.EMPTY);
  }
}
