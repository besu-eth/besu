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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.account.Accounts;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.Cluster;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.ClusterConfigurationBuilder;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.TransactionEncoder;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.utils.Numeric;

/**
 * Exercises, end-to-end, the snap/2 reorg-recovery path ({@code SnapV2ReorgHealer} via {@code
 * SnapV2WorldDownloadState.finishPivotCatchup}) during PoS snap sync.
 *
 * <p>Two miners build competing forks that share only a common block 1; the chain is Amsterdam at
 * genesis so every block carries a block access list (EIP-7928) and all nodes speak snap/2
 * (EIP-8189). Blocks 2-3 of each fork deploy ~2000 small storage-writing contracts from the same
 * benefactor nonces, so the pivot's world state is a large account-heavy delta over genesis and the
 * (heavily throttled) world-state download runs for several seconds — the window in which the pivot
 * is switched to fork B mid-download. (Amsterdam's EIP-8037 state-growth gas pricing makes new
 * slots ~111K gas each, so the block gas limit is raised to 1B on this devnet to fit a meaningful
 * state into two blocks.)
 *
 * <p>The forks are constructed so their state deltas hit every reorg scenario of the healing
 * decision matrix:
 *
 * <ul>
 *   <li><b>Touched on both forks</b>: the same contracts are deployed from the same benefactor
 *       nonces on both forks (hence identical addresses) but write different slot values. The
 *       canonical BAL must win: slots read fork B's values after sync.
 *   <li><b>Slot split across forks</b>: one contract writes 24 slots on fork A but only 12 on fork
 *       B. Slots 0-11 are overwritten by the canonical BAL; slots 12-23 existed only on the
 *       orphaned fork and must be re-fetched and removed.
 *   <li><b>Orphaned fork only</b>: contracts deployed and a transfer recipient created only on fork
 *       A must be deleted (code empty, balance zero) after recovery.
 *   <li><b>Canonical fork only</b>: a transfer recipient created only on fork B must exist with its
 *       balance after sync.
 *   <li><b>Untouched by both forks</b>: covered implicitly by everything else.
 * </ul>
 *
 * <p>The sync node snap-syncs toward fork A while connected only to miner A. Once fork A's Stage-1
 * header round completes <em>and the orphaned fork's BALs are persisted locally</em> (the reorg
 * healer reads orphaned BALs from local storage, so this is the snap/2-specific precondition this
 * test waits for via {@code eth_getBlockAccessList}), we connect miner A to miner B and point A's
 * forkchoice at fork B's (higher) head: A reorgs onto fork B and the sync node's single peer flips
 * forks. A pivot re-check during the still-running world-state download then adopts fork B's pivot;
 * the continuation chain round re-anchors below the fork-A pivot (the forks diverge at block 2),
 * downloads fork B's headers and BALs, and {@code finishPivotCatchup} detects that the old pivot is
 * no longer canonical and runs the reorg healer.
 *
 * <p>Final assertions go beyond "head matches fork B": the per-scenario state checks above run
 * against the synced node, and a fresh block built by miner B on top of fork B's head must validate
 * ({@code engine_newPayloadV5} → {@code VALID}) on the sync node — proof the healed world state is
 * production-ready.
 *
 * <p>The consensus layer is simulated over the Engine API (Amsterdam: {@code
 * engine_forkchoiceUpdatedV4} / {@code engine_getPayloadV6} / {@code engine_newPayloadV5}; JWT is
 * disabled in acceptance tests). No safe/finalized block is ever provided to the sync node, so the
 * pivot is picked 64 blocks behind the head (pure non-finality).
 *
 * <p><b>Timing sensitivity (flakiness risk):</b> as in {@link SnapSyncForkRecoveryAcceptanceTest},
 * the recovery only triggers if the pivot is switched to fork B <em>while fork A's round-1
 * world-state download is still running</em>. The window is created by the ~2000 contract accounts
 * (one storage task + one code task per account, serialized by the throttle flags) and the short
 * pivot-check interval. On a slow or heavily loaded CI agent this balance may need retuning (raise
 * {@link #CONTRACTS_PER_HEAVY_BLOCK}, or lower the check interval further) if the test becomes
 * flaky.
 */
public class SnapV2ReorgRecoveryAcceptanceTest extends AcceptanceTestBase {

  private static final MediaType MEDIA_TYPE_JSON =
      MediaType.parse("application/json; charset=utf-8");
  private static final String ZERO_HASH =
      "0x0000000000000000000000000000000000000000000000000000000000000000";

  // Fork geometry. Pivot = head - 64 (default pivot distance). pivot_A = 10, pivot_B = 15; the
  // forks share only block 1 and diverge at block 2 (distinct feeRecipient). The reorg healing
  // window is therefore blocks 2..10 on the orphaned fork vs 2..15 on the canonical fork; all
  // state-creating transactions are in blocks 2-3, comfortably below the old pivot.
  private static final int COMMON_HEIGHT = 1;
  private static final int FORK_A_HEIGHT = 74; // pivot_A = 10
  private static final int FORK_B_HEIGHT = 79; // pivot_B = 15

  // Window > 64 so repeated fork-A FCUs reuse pivot_A (distance stays 64), but the fork-B FCU
  // (distance 64 + (Hb-Ha)) forces a re-pivot to pivot_B.
  private static final int PIVOT_BLOCK_WINDOW_VALIDITY = 65;

  // State geometry: blocks 2 and 3 of each fork deploy CONTRACTS_PER_HEAVY_BLOCK contracts each,
  // every contract writing STORAGE_SLOTS_PER_CONTRACT slots. The snap/2 world-state download
  // processes one storage task and one code task per contract account; with the throttles below
  // that is ~2s per 1000 contracts, so 2000 contracts keep the download running for several
  // seconds — the window in which the pivot is switched to fork B. Note slots do NOT need to be
  // numerous: account count, not slot count, drives the download duration.
  private static final int HEAVY_BLOCK_1 = 2;
  private static final int HEAVY_BLOCK_2 = 3;
  private static final int CONTRACTS_PER_HEAVY_BLOCK = 1000;
  private static final int STORAGE_SLOTS_PER_CONTRACT = 2;
  private static final int SPLIT_SLOTS_FORK_A = 24;
  private static final int SPLIT_SLOTS_FORK_B = 12;
  // Per-deploy gas limit: a 2-slot deploy measures ~440K gas on Amsterdam (two ~111K state-growth
  // SSTOREs + create overhead), so 600K is comfortable. 24-slot deploys need ~2.7M.
  private static final long DEPLOY_GAS_LIMIT = 600_000L;
  private static final long SPLIT_DEPLOY_GAS_LIMIT = 4_000_000L;
  // Value transfer to a fresh address: 21,000 intrinsic regular gas (EIP-2780 TX_BASE + cold
  // account access + value cost) plus the EIP-2780/8037 NEW_ACCOUNT state-growth charge
  // (120 state bytes x 1,530 = 183,600, drawn from the same gas pool), so a bare 21,000 limit
  // exceptionally halts and the recipient never gets the funds.
  private static final long TRANSFER_GAS_LIMIT = 250_000L;
  private static final long CHAIN_ID = 1L;
  // The devnet block gas limit (raised from 30M because of EIP-8037 state-growth pricing).
  private static final String TARGET_GAS_LIMIT = "0x3b9aca00";

  // Nonce allocation on both forks (same benefactor, same order where the state must overlap):
  // nonces 0..1999 = heavy deploys (same contract addresses on both forks, different slot values),
  // nonce 2000 = split-slot contract (same address both forks, 24 slots on A vs 12 on B),
  // nonces 2001..2005 = orphaned-only deploys (fork A only), nonce 2006 = fork-A-only transfer,
  // nonce 2001 = fork-B-only transfer, nonce 2007 = post-sync transfer on fork B
  // (above the fork-A orphaned range, so the nonce is free of collision with any
  // re-inserted orphaned transaction; the pool automatically mines the gap txs
  // together with this one in the same block).
  private static final long SPLIT_CONTRACT_NONCE = 2 * CONTRACTS_PER_HEAVY_BLOCK;
  private static final long ORPHAN_CONTRACT_NONCE_START = SPLIT_CONTRACT_NONCE + 1;
  private static final int ORPHAN_CONTRACT_COUNT = 5;
  private static final long POST_SYNC_TRANSFER_NONCE =
      ORPHAN_CONTRACT_NONCE_START + ORPHAN_CONTRACT_COUNT + 1;

  // Fresh addresses that no genesis alloc or deployed contract can collide with.
  private static final String FORK_A_ONLY_RECIPIENT = "0x1000000000000000000000000000000000000001";
  private static final String FORK_B_ONLY_RECIPIENT = "0x1000000000000000000000000000000000000002";
  private static final BigInteger TRANSFER_WEI = BigInteger.TEN.pow(18);

  private static final String BENEFACTOR_ADDRESS = "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73";

  // Block-build retry bounds (see buildBlock): each attempt runs a fresh build with a longer
  // window, so a heavily loaded miner still gets time to pack all 1000 deploys. A 1000-tx block
  // takes ~8s to build on an unloaded machine.
  private static final int MAX_BUILD_ATTEMPTS = 10;
  private static final long MAX_BUILD_WINDOW_MILLIS = 16_000L;
  // Empty blocks are ready almost immediately, so they always use this short, fixed window.
  private static final long EMPTY_BUILD_WINDOW_MILLIS = 150L;
  // Starting window for transaction-bearing blocks, before any adaptive growth (see
  // transactionBlockBuildWindowMillis).
  private static final long INITIAL_BUILD_WINDOW_MILLIS = 4_000L;

  private static final String FEE_RECIPIENT_A = "0x1111111111111111111111111111111111111111";
  private static final String FEE_RECIPIENT_B = "0x2222222222222222222222222222222222222222";

  private final OkHttpClient httpClient = new OkHttpClient();
  private final ObjectMapper mapper = new ObjectMapper();

  private Cluster noDiscoveryCluster;

  // Fork B's execution payloads, kept so we can pre-cache their headers on the sync node.
  private final List<ObjectNode> forkBBlocks = new ArrayList<>();
  // Each fork B payload's engine_getPayloadV6 executionRequests (parallel to forkBBlocks), needed
  // for the engine_newPayloadV5 pre-cache calls.
  private final List<String> forkBExecutionRequests = new ArrayList<>();
  // Fork A's head payload, cached so its header can be pre-cached on the sync node.
  private BuildResult forkAHeadResult;

  // Benefactor nonces for the two forks (they diverge after nonce 2000).
  private long nonceForkA = 0;
  private long nonceForkB = 0;

  // Adaptive build window for transaction-bearing blocks: remembered across blocks so that once one
  // heavy block proves a longer window is needed on this (possibly loaded) machine, later heavy
  // blocks start from that proven duration instead of re-climbing from the minimum each time.
  private long transactionBlockBuildWindowMillis = INITIAL_BUILD_WINDOW_MILLIS;

  @Test
  public void recoversFromReorgPastPivotDuringSnapV2Sync() throws Exception {
    final String genesis = loadAmsterdamGenesis();

    noDiscoveryCluster =
        new Cluster(new ClusterConfigurationBuilder().awaitPeerDiscovery(false).build(), net);

    final BesuNode minerA = createMiner("minerA", genesis);
    final BesuNode minerB = createMiner("minerB", genesis);
    final BesuNode syncNode = createSyncNode("syncNode", genesis);

    noDiscoveryCluster.start(minerA, minerB, syncNode);

    // Common prefix: build block 1 on A (empty) and import it into B so both forks share it.
    final BuildResult commonBlock = buildBlock(minerA, FEE_RECIPIENT_A, 0, COMMON_HEIGHT);
    importBlock(minerB, commonBlock);
    minerA.verify(blockchain.currentHeight(COMMON_HEIGHT));
    minerB.verify(blockchain.currentHeight(COMMON_HEIGHT));

    // Fork A on minerA, fork B on minerB. Distinct feeRecipient => divergent block 2. Both forks
    // deploy the same-nonce contracts below their pivots (touched on both forks, same addresses)
    // but with different slot values; fork A additionally deploys orphaned-only contracts and a
    // transfer (only existed on fork A), fork B a canonical-only transfer (only on fork B).
    final String forkAHeadHash = buildForkAChain(minerA);
    final String forkBHeadHash = buildForkBChain(minerB);

    // Pre-cache every fork-B header on the sync node so its pivot selector can resolve fork B's
    // pivot instantly from cache (no peer round-trip). Inert until we point the CL at fork B.
    for (int i = 0; i < forkBBlocks.size(); i++) {
      sendNewPayload(syncNode, forkBBlocks.get(i), forkBExecutionRequests.get(i));
    }

    // Capture console now so we only accumulate sync-phase logs (only the sync node emits the
    // snap/2 pivot catch-up / reorg lines we key off of).
    noDiscoveryCluster.startConsoleCapture();

    // Phase 1: snap-sync the sync node toward fork A, connected only to miner A.
    syncNode.execute(adminTransactions.addPeer(minerA.enodeUrl()));
    syncNode.verify(net.awaitPeerCount(1));

    sendNewPayload(
        syncNode, forkAHeadResult.executionPayload(), forkAHeadResult.executionRequests());
    // Resend the fork-A FCU (fast poll) until Stage 1 (backward header download) for the fork-A
    // pivot completes.
    await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofMillis(50))
        .until(
            () -> {
              fcuHeadOnly(syncNode, forkAHeadHash);
              return noDiscoveryCluster
                  .peekConsoleContents()
                  .contains("Header import progress 100.00%");
            });

    // snap/2 precondition: the reorg healer reads ORPHANED-fork BALs from local storage, so fork
    // A's BALs below the pivot must be persisted before we trigger the reorg. The chain download
    // persists BALs (anchor -> pivot) right after Stage 1; poll eth_getBlockAccessList for a
    // below-pivot fork-A block until it resolves (non-null result, no error).
    await()
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> hasBlockAccessList(syncNode, "0x2"));

    // Phase 2: connect miner A to miner B and point A's forkchoice at fork B's head: A
    // backward-syncs fork B from B and reorgs onto it, so the sync node's single peer flips from
    // fork A to fork B. Point the sync node's CL at fork B too; its cached fork-B headers let the
    // frequently-rechecked pivot advance onto fork B during round 1's long world-state download.
    // The continuation round re-anchors below the fork-A pivot, downloads fork B's headers and
    // BALs, and the pivot catch-up then detects the old pivot is no longer canonical -> snap/2
    // reorg recovery.
    minerA.execute(adminTransactions.addPeer(minerB.enodeUrl()));
    fcuHeadOnly(minerA, forkBHeadHash);
    fcuHeadOnly(syncNode, forkBHeadHash);

    // Keep nudging A onto fork B (its backward-sync + reorg takes a moment) and the sync node's CL
    // at fork B until the pivot catch-up runs the reorg path.
    try {
      await()
          .atMost(Duration.ofMinutes(4))
          .pollInterval(Duration.ofMillis(250))
          .until(
              () -> {
                fcuHeadOnly(minerA, forkBHeadHash);
                fcuHeadOnly(syncNode, forkBHeadHash);
                return noDiscoveryCluster
                    .peekConsoleContents()
                    .contains("snap/2 chain reorg detected at pivot catch-up");
              });
    } catch (final Throwable t) {
      System.out.println(
          "REORG DETECTION TIMED OUT - sync console so far:\n"
              + noDiscoveryCluster.peekConsoleContents());
      throw t;
    }

    // Recovery succeeded: the sync node fully adopts fork B (head number and hash match), after
    // the reorg healer corrected the partially-downloaded world state and the remaining ranges
    // were downloaded at the fork-B pivot.
    try {
      await()
          .atMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofSeconds(2))
          .until(
              () -> {
                fcuHeadOnly(syncNode, forkBHeadHash);
                final EthBlock.Block head =
                    syncNode.execute(
                        ethTransactions.block(
                            DefaultBlockParameter.valueOf(BigInteger.valueOf(FORK_B_HEIGHT))));
                return head != null && forkBHeadHash.equals(head.getHash());
              });
    } catch (final Throwable t) {
      System.out.println(
          "SYNC COMPLETION TIMED OUT - sync console so far:\n"
              + noDiscoveryCluster.peekConsoleContents());
      throw t;
    }

    final String syncConsole = noDiscoveryCluster.peekConsoleContents();
    assertThat(syncConsole).contains("snap/2 reorg recovery complete");
    assertThat(syncConsole).doesNotContain("snap/2 pivot catch-up failed");

    final EthBlock.Block syncedHead =
        syncNode.execute(
            ethTransactions.block(
                DefaultBlockParameter.valueOf(BigInteger.valueOf(FORK_B_HEIGHT))));
    assertThat(syncedHead.getHash()).isEqualTo(forkBHeadHash);

    // --- State assertions on the healed world state ---
    final Address benefactor = Address.fromHexString(BENEFACTOR_ADDRESS);

    // Touched on both forks: a contract deployed on both forks (nonce 0) must hold fork B's slot
    // values (2),
    // not fork A's (1) — the canonical BAL won over the orphaned download.
    final Account heavyContract0 =
        Account.create(ethTransactions, Address.contractAddress(benefactor, 0));
    assertThat(syncNode.execute(ethTransactions.getStorageAt(heavyContract0, BigInteger.ZERO)))
        .isEqualTo(storageValueHex(2));
    assertThat(syncNode.execute(ethTransactions.getStorageAt(heavyContract0, BigInteger.ONE)))
        .isEqualTo(storageValueHex(2));

    // Slot split across forks: the split contract wrote 24 slots on fork A but only 12 on fork B.
    // An overlapping slot reads fork B's value; a slot that existed only on the orphaned fork was
    // re-fetched from the canonical chain and removed.
    final Account splitContract =
        Account.create(ethTransactions, Address.contractAddress(benefactor, SPLIT_CONTRACT_NONCE));
    assertThat(syncNode.execute(ethTransactions.getStorageAt(splitContract, BigInteger.ZERO)))
        .isEqualTo(storageValueHex(2));
    assertThat(
            syncNode.execute(ethTransactions.getStorageAt(splitContract, BigInteger.valueOf(20))))
        .isEqualTo(storageValueHex(0));

    // Orphaned fork only: a contract deployed only on the orphaned fork must be gone entirely.
    final Account orphanContract =
        Account.create(
            ethTransactions, Address.contractAddress(benefactor, ORPHAN_CONTRACT_NONCE_START));
    assertThat(syncNode.execute(ethTransactions.getCode(orphanContract))).isEqualTo(Bytes.EMPTY);
    // Orphaned fork only: the fork-A-only transfer recipient must not exist (balance zero).
    final Account forkAOnlyRecipient =
        Account.create(ethTransactions, Address.fromHexString(FORK_A_ONLY_RECIPIENT));
    assertThat(syncNode.execute(ethTransactions.getBalance(forkAOnlyRecipient)))
        .isEqualTo(BigInteger.ZERO);

    // Canonical fork only: the fork-B-only transfer recipient must exist with its canonical
    // balance.
    final Account forkBOnlyRecipient =
        Account.create(ethTransactions, Address.fromHexString(FORK_B_ONLY_RECIPIENT));
    assertThat(syncNode.execute(ethTransactions.getBalance(forkBOnlyRecipient)))
        .isEqualTo(TRANSFER_WEI);

    // Production-readiness: a fresh block built by miner B on top of fork B's head (containing a
    // transfer that re-creates the fork-A-only recipient via normal execution) must validate on
    // the sync node — the healed world state can execute new blocks.
    //
    // The reorg re-inserted orphaned fork-A deploys (nonces 2002-2005) and a transfer
    // (nonce 2006) into the pool. (The orphaned deploy at nonce 2001 is below fork B's
    // next benefactor nonce and dropped.) Replace the surviving orphaned txs (nonces
    // 2002-2006, including the transfer at 2006) with harmless self-transfers so they
    // don't re-deploy the contracts that were deleted by the reorg healer (orphaned fork
    // only) or re-send the fork-A-only transfer. Use a higher gas price (2,000) to
    // override the orphaned txs, then submit the real post-sync transfer at nonce 2007.
    final Credentials benefactorCreds =
        Credentials.create(Accounts.GENESIS_ACCOUNT_ONE_PRIVATE_KEY);
    for (long n = ORPHAN_CONTRACT_NONCE_START + 1;
        n <= ORPHAN_CONTRACT_NONCE_START + ORPHAN_CONTRACT_COUNT;
        n++) {
      final RawTransaction gapTx =
          RawTransaction.createEtherTransaction(
              BigInteger.valueOf(n),
              BigInteger.valueOf(2_000),
              BigInteger.valueOf(21_000),
              BENEFACTOR_ADDRESS, // self-transfer: harmless gap-filler
              BigInteger.ZERO);
      minerB.execute(
          ethTransactions.sendRawTransaction(
              Numeric.toHexString(
                  TransactionEncoder.signMessage(gapTx, CHAIN_ID, benefactorCreds))));
    }
    submitTransfer(minerB, POST_SYNC_TRANSFER_NONCE, FORK_A_ONLY_RECIPIENT);
    // Expected tx count: 5 gap-fillers at nonces 2002-2006 (replacing the 4 orphaned
    // deploys and the orphaned transfer) + 1 post-sync transfer at 2007 = 6
    final BuildResult block80 = buildBlock(minerB, FEE_RECIPIENT_B, 6, FORK_B_HEIGHT + 1);
    sendNewPayloadExpectValid(syncNode, block80.executionPayload(), block80.executionRequests());
    final String block80Hash = block80.executionPayload().get("blockHash").asText();
    fcuHeadOnly(syncNode, block80Hash);
    syncNode.verify(blockchain.currentHeight(FORK_B_HEIGHT + 1));
    assertThat(syncNode.execute(ethTransactions.getBalance(forkAOnlyRecipient)))
        .isEqualTo(TRANSFER_WEI);
  }

  private static int forkATxCount(final int height) {
    if (height == HEAVY_BLOCK_1) {
      return CONTRACTS_PER_HEAVY_BLOCK;
    }
    if (height == HEAVY_BLOCK_2) {
      return CONTRACTS_PER_HEAVY_BLOCK + 1 + ORPHAN_CONTRACT_COUNT + 1;
    }
    return 0;
  }

  private static int forkBTxCount(final int height) {
    if (height == HEAVY_BLOCK_1) {
      return CONTRACTS_PER_HEAVY_BLOCK;
    }
    if (height == HEAVY_BLOCK_2) {
      return CONTRACTS_PER_HEAVY_BLOCK + 1 + 1;
    }
    return 0;
  }

  /**
   * Submits fork A's transactions for one heavy block: 1000 heavy deploys (or, on the second heavy
   * block, additionally the split-slot deploy, the orphaned-only deploys, and the fork-A-only
   * transfer). Nonces are consumed in submission order; by the second heavy block the counter has
   * reached {@link #SPLIT_CONTRACT_NONCE} exactly.
   */
  private void submitForkATransactions(final BesuNode miner, final int height) {
    for (int i = 0; i < CONTRACTS_PER_HEAVY_BLOCK; i++) {
      submitDeploy(
          miner,
          nonceForkA++,
          storageContractInitCode(STORAGE_SLOTS_PER_CONTRACT, 1),
          DEPLOY_GAS_LIMIT);
    }
    if (height == HEAVY_BLOCK_2) {
      // Split-slot contract: 24 slots on fork A (vs 12 on fork B), same address on both forks.
      submitDeploy(
          miner,
          nonceForkA++,
          storageContractInitCode(SPLIT_SLOTS_FORK_A, 1),
          SPLIT_DEPLOY_GAS_LIMIT);
      // Orphaned-only contracts: no counterpart on fork B.
      for (int i = 0; i < ORPHAN_CONTRACT_COUNT; i++) {
        submitDeploy(
            miner,
            nonceForkA++,
            storageContractInitCode(STORAGE_SLOTS_PER_CONTRACT, 1),
            DEPLOY_GAS_LIMIT);
      }
      // Fork-A-only transfer recipient (orphaned fork only).
      submitTransfer(miner, nonceForkA++, FORK_A_ONLY_RECIPIENT);
    }
  }

  /** Submits fork B's transactions for one heavy block (mirror of fork A minus the orphans). */
  private void submitForkBTransactions(final BesuNode miner, final int height) {
    for (int i = 0; i < CONTRACTS_PER_HEAVY_BLOCK; i++) {
      submitDeploy(
          miner,
          nonceForkB++,
          storageContractInitCode(STORAGE_SLOTS_PER_CONTRACT, 2),
          DEPLOY_GAS_LIMIT);
    }
    if (height == HEAVY_BLOCK_2) {
      // Split-slot contract: only 12 slots on fork B, same address as on fork A.
      submitDeploy(
          miner,
          nonceForkB++,
          storageContractInitCode(SPLIT_SLOTS_FORK_B, 2),
          SPLIT_DEPLOY_GAS_LIMIT);
      // Fork-B-only transfer recipient (canonical fork only).
      submitTransfer(miner, nonceForkB++, FORK_B_ONLY_RECIPIENT);
    }
  }

  private String buildForkAChain(final BesuNode minerA) throws IOException {
    for (int height = COMMON_HEIGHT + 1; height <= FORK_A_HEIGHT; height++) {
      final int txCount = forkATxCount(height);
      if (txCount > 0) {
        submitForkATransactions(minerA, height);
      }
      forkAHeadResult = buildBlock(minerA, FEE_RECIPIENT_A, txCount, height);
    }
    minerA.verify(blockchain.currentHeight(FORK_A_HEIGHT));
    return forkAHeadResult.executionPayload().get("blockHash").asText();
  }

  private String buildForkBChain(final BesuNode minerB) throws IOException {
    BuildResult head = null;
    for (int height = COMMON_HEIGHT + 1; height <= FORK_B_HEIGHT; height++) {
      final int txCount = forkBTxCount(height);
      if (txCount > 0) {
        submitForkBTransactions(minerB, height);
      }
      head = buildBlock(minerB, FEE_RECIPIENT_B, txCount, height);
      forkBBlocks.add(head.executionPayload());
      forkBExecutionRequests.add(head.executionRequests());
    }
    minerB.verify(blockchain.currentHeight(FORK_B_HEIGHT));
    return head.executionPayload().get("blockHash").asText();
  }

  private void submitDeploy(
      final BesuNode miner, final long nonce, final String initCode, final long gasLimit) {
    final Credentials benefactor = Credentials.create(Accounts.GENESIS_ACCOUNT_ONE_PRIVATE_KEY);
    final RawTransaction tx =
        RawTransaction.createContractTransaction(
            BigInteger.valueOf(nonce),
            BigInteger.valueOf(1_000), // gas price (wei), above the base fee
            BigInteger.valueOf(gasLimit), // enough for the SSTOREs + deploy
            BigInteger.ZERO,
            initCode);
    final String signed =
        Numeric.toHexString(TransactionEncoder.signMessage(tx, CHAIN_ID, benefactor));
    miner.execute(ethTransactions.sendRawTransaction(signed));
  }

  private void submitTransfer(final BesuNode miner, final long nonce, final String recipient) {
    final Credentials benefactor = Credentials.create(Accounts.GENESIS_ACCOUNT_ONE_PRIVATE_KEY);
    final RawTransaction tx =
        RawTransaction.createEtherTransaction(
            BigInteger.valueOf(nonce),
            BigInteger.valueOf(1_000),
            BigInteger.valueOf(TRANSFER_GAS_LIMIT),
            recipient,
            TRANSFER_WEI);
    final String signed =
        Numeric.toHexString(TransactionEncoder.signMessage(tx, CHAIN_ID, benefactor));
    miner.execute(ethTransactions.sendRawTransaction(signed));
  }

  /**
   * EVM init code that writes {@code slotCount} storage slots (slot j = {@code slotValue}) in the
   * constructor, then returns a 1-byte runtime. Unrolled (no loop) to keep the bytecode trivial.
   * Slot indices and values must fit in one byte.
   */
  private static String storageContractInitCode(final int slotCount, final int slotValue) {
    final StringBuilder code = new StringBuilder("0x");
    for (int j = 0; j < slotCount; j++) {
      // PUSH1 value ; PUSH1 j ; SSTORE. PUSH1 operands are single bytes, so both the slot index
      // and the value must fit in one byte.
      code.append("60")
          .append(String.format("%02x", slotValue))
          .append("60")
          .append(String.format("%02x", j))
          .append("55");
    }
    // PUSH1 0 ; PUSH1 0 ; MSTORE ; PUSH1 1 ; PUSH1 31 ; RETURN  -> return a 1-byte (0x00) runtime
    code.append("60006000526001601ff3");
    return code.toString();
  }

  private static String storageValueHex(final int value) {
    return "0x" + "0".repeat(63) + Integer.toHexString(value);
  }

  private record BuildResult(ObjectNode executionPayload, String executionRequests) {}

  /**
   * Drives a single PoS block build on {@code miner} over the Engine API (Amsterdam: FCU V4 +
   * getPayloadV6 + newPayloadV5) and returns the execution payload and its execution requests. Any
   * pending pooled transactions are included in the block.
   */
  private BuildResult buildBlock(
      final BesuNode miner,
      final String feeRecipient,
      final int expectedTxCount,
      final long slotNumber)
      throws IOException {
    final EthBlock.Block head = miner.execute(ethTransactions.block());
    final String headHash = head.getHash();
    final long baseTimestamp = head.getTimestamp().longValue() + 1;

    final boolean hasTransactions = expectedTxCount > 0;
    // Empty blocks always use the short fixed window; transaction-bearing blocks start from the
    // adaptively-remembered window and grow it per attempt if the build was still incomplete.
    long buildWindowMillis =
        hasTransactions ? transactionBlockBuildWindowMillis : EMPTY_BUILD_WINDOW_MILLIS;
    ObjectNode executionPayload = null;
    String executionRequests = "[]";
    for (int attempt = 0; attempt < MAX_BUILD_ATTEMPTS; attempt++) {
      // Distinct timestamp per attempt => distinct payload id => a fresh, uncancelled build over
      // the full transaction pool, rather than the deduplicated, already-finalized previous build.
      final String payloadId =
          startBlockBuild(miner, headHash, baseTimestamp + attempt, feeRecipient, slotNumber);
      sleep(buildWindowMillis);
      final ObjectNode getPayloadResult = fetchPayload(miner, payloadId);
      final ObjectNode payload = (ObjectNode) getPayloadResult.get("executionPayload");
      if (payload.get("transactions").size() == expectedTxCount) {
        executionPayload = payload;
        final JsonNode requests = getPayloadResult.get("executionRequests");
        executionRequests = requests != null && !requests.isNull() ? requests.toString() : "[]";
        break;
      }
      buildWindowMillis = Math.min(buildWindowMillis * 2, MAX_BUILD_WINDOW_MILLIS);
    }
    assertThat(executionPayload)
        .as(
            "miner did not build a block with %s transaction(s) within %s attempts",
            expectedTxCount, MAX_BUILD_ATTEMPTS)
        .isNotNull();

    // Remember the window that worked for transaction-bearing blocks so later heavy blocks start
    // from a proven-sufficient duration. Empty blocks never feed back into it.
    if (hasTransactions) {
      transactionBlockBuildWindowMillis = buildWindowMillis;
    }

    final BuildResult result = new BuildResult(executionPayload, executionRequests);
    importBlock(miner, result);
    return result;
  }

  /**
   * engine_forkchoiceUpdatedV4 with Amsterdam payload attributes; returns the payload id for the
   * build.
   */
  private String startBlockBuild(
      final BesuNode miner,
      final String headHash,
      final long timestamp,
      final String feeRecipient,
      final long slotNumber)
      throws IOException {
    final String fcuWithAttributes =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV4\",\"params\":["
            + "{\"headBlockHash\":\""
            + headHash
            + "\",\"safeBlockHash\":\""
            + headHash
            + "\",\"finalizedBlockHash\":\""
            + ZERO_HASH
            + "\"},"
            + "{\"timestamp\":\"0x"
            + Long.toHexString(timestamp)
            + "\",\"prevRandao\":\""
            + ZERO_HASH
            + "\",\"suggestedFeeRecipient\":\""
            + feeRecipient
            + "\",\"withdrawals\":[],\"parentBeaconBlockRoot\":\""
            + ZERO_HASH
            + "\",\"slotNumber\":\"0x"
            + Long.toHexString(slotNumber)
            + "\",\"targetGasLimit\":\""
            + TARGET_GAS_LIMIT
            + "\"}],\"id\":67}";
    try (Response response = engineCall(miner, fcuWithAttributes).execute()) {
      assertThat(response.code()).isEqualTo(200);
      final String payloadId = result(response).get("payloadId").asText();
      assertThat(payloadId).isNotEmpty();
      return payloadId;
    }
  }

  /**
   * engine_getPayloadV6 for the given payload id; returns the full result object ({@code
   * executionPayload}, {@code executionRequests}, ...).
   */
  private ObjectNode fetchPayload(final BesuNode miner, final String payloadId) throws IOException {
    final String getPayload =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_getPayloadV6\",\"params\":[\""
            + payloadId
            + "\"],\"id\":67}";
    try (Response response = engineCall(miner, getPayload).execute()) {
      assertThat(response.code()).isEqualTo(200);
      return (ObjectNode) result(response);
    }
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /** engine_newPayloadV5 (VALID) + engine_forkchoiceUpdatedV4 to make the block canonical. */
  private void importBlock(final BesuNode node, final BuildResult build) throws IOException {
    final String blockHash = build.executionPayload().get("blockHash").asText();
    final String newPayload =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_newPayloadV5\",\"params\":["
            + build.executionPayload()
            + ",[],\""
            + ZERO_HASH
            + "\","
            + build.executionRequests()
            + "],\"id\":67}";
    try (Response response = engineCall(node, newPayload).execute()) {
      assertThat(response.code()).isEqualTo(200);
      assertThat(result(response).get("status").asText())
          .as("engine_newPayloadV5 for block %s", blockHash)
          .isEqualTo("VALID");
    }
    final String fcu =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV4\",\"params\":["
            + "{\"headBlockHash\":\""
            + blockHash
            + "\",\"safeBlockHash\":\""
            + blockHash
            + "\",\"finalizedBlockHash\":\""
            + ZERO_HASH
            + "\"},null],\"id\":67}";
    try (Response response = engineCall(node, fcu).execute()) {
      assertThat(response.code()).isEqualTo(200);
      assertThat(result(response).get("payloadStatus").get("status").asText()).isEqualTo("VALID");
    }
  }

  /**
   * Submits a payload to a (syncing) node so it caches the header; status may be SYNCING/ACCEPTED.
   */
  private void sendNewPayload(
      final BesuNode node, final ObjectNode executionPayload, final String executionRequests)
      throws IOException {
    final String newPayload =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_newPayloadV5\",\"params\":["
            + executionPayload
            + ",[],\""
            + ZERO_HASH
            + "\","
            + executionRequests
            + "],\"id\":67}";
    try (Response response = engineCall(node, newPayload).execute()) {
      assertThat(response.code()).isEqualTo(200);
    }
  }

  /** Like {@link #sendNewPayload} but requires the payload to validate (post-sync node). */
  private void sendNewPayloadExpectValid(
      final BesuNode node, final ObjectNode executionPayload, final String executionRequests)
      throws IOException {
    final String newPayload =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_newPayloadV5\",\"params\":["
            + executionPayload
            + ",[],\""
            + ZERO_HASH
            + "\","
            + executionRequests
            + "],\"id\":67}";
    try (Response response = engineCall(node, newPayload).execute()) {
      assertThat(response.code()).isEqualTo(200);
      assertThat(result(response).get("status").asText()).isEqualTo("VALID");
    }
  }

  /** forkchoiceUpdatedV4 with head only (no safe/finalized, no attributes): pure non-finality. */
  private void fcuHeadOnly(final BesuNode node, final String headHash) throws IOException {
    final String fcu =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV4\",\"params\":["
            + "{\"headBlockHash\":\""
            + headHash
            + "\",\"safeBlockHash\":\""
            + ZERO_HASH
            + "\",\"finalizedBlockHash\":\""
            + ZERO_HASH
            + "\"},null],\"id\":67}";
    try (Response response = engineCall(node, fcu).execute()) {
      assertThat(response.code()).isEqualTo(200);
    }
  }

  /**
   * Polls {@code eth_getBlockAccessList} by block number; {@code true} once the node resolves it to
   * an actual BAL (non-null result), i.e. the BAL for that block is persisted locally.
   */
  private boolean hasBlockAccessList(final BesuNode node, final String blockNumberHex)
      throws IOException {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockAccessList\",\"params\":[\""
            + blockNumberHex
            + "\"],\"id\":67}";
    try (Response response = jsonRpcCall(node, request).execute()) {
      if (response.code() != 200 || response.body() == null) {
        return false;
      }
      final JsonNode tree = mapper.readTree(response.body().string());
      final JsonNode result = tree.get("result");
      return result != null && !result.isNull();
    }
  }

  private JsonNode result(final Response response) throws IOException {
    return mapper.readTree(response.body().string()).get("result");
  }

  private Call engineCall(final BesuNode node, final String request) {
    return httpClient.newCall(
        new Request.Builder()
            .url(node.engineRpcUrl().get())
            .post(RequestBody.create(request, MEDIA_TYPE_JSON))
            .build());
  }

  private Call jsonRpcCall(final BesuNode node, final String request) {
    return httpClient.newCall(
        new Request.Builder()
            .url(node.jsonRpcBaseUrl().get())
            .post(RequestBody.create(request, MEDIA_TYPE_JSON))
            .build());
  }

  private BesuNode createMiner(final String name, final String genesis) throws IOException {
    final SnapSyncConfiguration snapServerEnabled =
        ImmutableSnapSyncConfiguration.builder().isSnapServerEnabled(true).build();
    final BesuNode node =
        besu.createNode(
            name,
            b ->
                b.devMode(false)
                    .genesisConfigProvider(unused -> Optional.of(genesis))
                    .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                    .engineRpcEnabled(true)
                    .jsonRpcEnabled()
                    .jsonRpcAdmin()
                    .jsonRpcTxPool()
                    .discoveryEnabled(false)
                    .bootnodeEligible(false)
                    .miningEnabled()
                    .extraCLIOptions(
                        List.of(
                            // Advertise the snap/2 capability so the sync node can fetch BALs and
                            // state ranges over snap/2 from this node.
                            "--Xsnap2-enabled",
                            // Both heavy blocks carry 1000+ txs from the single benefactor, far
                            // above the default per-sender future queue (200).
                            "--tx-pool-max-future-by-sender=5000")));
    node.setSynchronizerConfiguration(
        SynchronizerConfiguration.builder()
            .syncMode(SyncMode.FULL)
            .syncMinimumPeerCount(1)
            .snapSyncConfiguration(snapServerEnabled)
            .build());
    return node;
  }

  private BesuNode createSyncNode(final String name, final String genesis) throws IOException {
    final BesuNode node =
        besu.createNode(
            name,
            b ->
                b.devMode(false)
                    .genesisConfigProvider(unused -> Optional.of(genesis))
                    .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                    .engineRpcEnabled(true)
                    .jsonRpcEnabled()
                    .jsonRpcAdmin()
                    .discoveryEnabled(false)
                    .bootnodeEligible(false)
                    // Enable the snap/2 downloader (BAL-based catch-up, no trie healing).
                    // Re-check the snap pivot very frequently (default is once per minute, far
                    // longer than this whole test) so the pivot can actually be re-evaluated and
                    // switched to fork B while round 1's world-state download is still running.
                    // Also serialize the world-state download pipelines so the download runs long
                    // enough for a re-check to fire during it.
                    //
                    // Lower the world-state stall thresholds too, so any genuine dead-end during
                    // the retargeting clears in seconds rather than minutes. Every throttled
                    // request in the normal phases makes progress (resetting the counter), so only
                    // a genuine dead-end accumulates.
                    .extraCLIOptions(
                        List.of(
                            "--Xsnap2-enabled",
                            "--Xsnapsync-synchronizer-pivot-block-check-interval-millis=100",
                            "--Xsynchronizer-world-state-request-parallelism=1",
                            "--Xsynchronizer-world-state-hash-count-per-request=1",
                            "--Xsnapsync-synchronizer-storage-count-per-request=1",
                            "--Xsnapsync-synchronizer-bytecode-count-per-request=1",
                            "--Xsynchronizer-world-state-max-requests-without-progress=50",
                            "--Xsynchronizer-world-state-min-millis-before-stalling=10000")));
    node.setSynchronizerConfiguration(
        SynchronizerConfiguration.builder()
            .syncMode(SyncMode.SNAP)
            .syncMinimumPeerCount(1)
            .snapSyncConfiguration(
                ImmutableSnapSyncConfiguration.builder()
                    .isSnapServerEnabled(true)
                    .pivotBlockWindowValidity(PIVOT_BLOCK_WINDOW_VALIDITY)
                    .build())
            .build());
    return node;
  }

  /**
   * Loads the merged-at-genesis Amsterdam genesis (TTD=0, all forks through Amsterdam active at
   * genesis so every block carries a block access list and snap/2 applies). The alloc contains a
   * single prefunded benefactor plus the Prague system contracts (deposit / withdrawal request /
   * consolidation request, with their initialized storage) — block building fails with {@code
   * SystemCallNoCodeAtAddressException} without them. The snap-syncable state comes from
   * transactions below the pivot, not from the genesis alloc (the sync node already holds the
   * genesis state locally).
   */
  private static String loadAmsterdamGenesis() {
    try (var in =
        SnapV2ReorgRecoveryAcceptanceTest.class.getResourceAsStream(
            "/snapsync/snap_v2_reorg_genesis.json")) {
      assertThat(in).as("snap_v2_reorg_genesis.json on the test classpath").isNotNull();
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  @Override
  public void tearDownAcceptanceTestBase() {
    if (noDiscoveryCluster != null) {
      noDiscoveryCluster.stop();
    }
    super.tearDownAcceptanceTestBase();
  }
}
