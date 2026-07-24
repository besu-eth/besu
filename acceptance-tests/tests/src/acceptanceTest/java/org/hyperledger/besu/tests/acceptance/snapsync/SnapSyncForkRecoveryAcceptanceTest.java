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

import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Accounts;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.Cluster;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.ClusterConfigurationBuilder;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.TransactionEncoder;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.utils.Numeric;

/**
 * Reproduces, end-to-end, the reorg-recovery path in {@code BackwardHeaderDriver} during PoS snap
 * sync.
 *
 * <p>Two miners build competing forks that share only a common block 1. Fork A's blocks below its
 * pivot deploy many small storage-writing contracts, so the pivot's world state is a large
 * storage-heavy delta over genesis. The sync node's world-state download is throttled hard (one
 * request/item at a time), which — because storage download is throttleable, unlike account-range
 * download — makes its round-1 world-state download run for several seconds. The snap pivot
 * selector is re-checked frequently (a small configurable interval, vs the 1-minute default) during
 * that window.
 *
 * <p>The sync node snap-syncs toward fork A while connected only to miner A. Once fork A's Stage-1
 * header round completes, we connect miner A to miner B and point A's forkchoice at fork B's
 * (higher) head: A backward-syncs fork B from B and reorgs onto it, so the sync node's single peer
 * flips from fork A to fork B (no competing-peer header corruption). We also point the sync node's
 * consensus layer at fork B; a pivot re-check during the still-running round-1 world-state download
 * then adopts fork B, and snap runs a continuation Stage-1 round whose anchor is the previous (fork
 * A) pivot. Because the forks diverge below that anchor, the boundary header does not link to the
 * stored anchor and {@code BackwardHeaderDriver} enters recovery ("Anchor mismatch at #..."), walks
 * back to the common ancestor, and the node ultimately syncs fully to fork B.
 *
 * <p>The consensus layer is simulated over the Engine API (JWT is disabled in acceptance tests). No
 * safe/finalized block is ever provided, so {@code PivotSelectorFromSafeBlock} picks the pivot 64
 * blocks behind the head (pure non-finality).
 *
 * <p><b>Timing sensitivity (flakiness risk):</b> the recovery only triggers if the pivot is
 * switched to fork B <em>while fork A's round-1 world-state download is still running</em>. If that
 * download finishes first, the node completes on fork A, no continuation round runs, and the
 * "Anchor mismatch" assertion times out. The window is widened by the storage-heavy delta ({@link
 * #CONTRACTS_PER_HEAVY_BLOCK}/{@link #STORAGE_SLOTS_PER_CONTRACT}), the aggressive world-state
 * throttle flags, and the short pivot-check interval — all set below. On a slow or heavily loaded
 * CI agent this balance may need retuning (increase the heavy-block weighting, or lower the check
 * interval further) if the test becomes flaky.
 */
public class SnapSyncForkRecoveryAcceptanceTest extends AcceptanceTestBase {

  private static final MediaType MEDIA_TYPE_JSON =
      MediaType.parse("application/json; charset=utf-8");
  private static final String ZERO_HASH =
      "0x0000000000000000000000000000000000000000000000000000000000000000";

  // Fork geometry. Pivot = head - 64 (PIVOT_DISTANCE). pivot_A = 10, pivot_B = 15; recovery matches
  // block 1. Forks share only block 1 and diverge at block 2 (distinct feeRecipient). Pivots are
  // kept low to minimise the number of (mostly empty, PIVOT_DISTANCE filler) blocks to build.
  private static final int COMMON_HEIGHT = 1;
  private static final int FORK_A_HEIGHT = 74; // pivot_A = 10
  private static final int FORK_B_HEIGHT = 79; // pivot_B = 15

  // Window > 64 so repeated fork-A FCUs reuse pivot_A (distance stays 64), but the fork-B FCU
  // (distance 64 + (Hb-Ha)) forces a re-pivot to pivot_B.
  private static final int PIVOT_BLOCK_WINDOW_VALIDITY = 65;

  // Fork A's blocks below the pivot (2..pivot_A) each deploy several small contracts that write
  // many
  // storage slots. This makes the pivot's world state a large STORAGE-heavy delta over genesis.
  // Unlike plain accounts (whose range download can't be throttled), storage download IS throttled
  // (one slot per request, one request at a time), so the sync node's round-1 world-state download
  // runs for several seconds — the window during which the frequently re-checked pivot is switched
  // to fork B while round 1 is still active (a recoverable continuation, anchor = the fork-A
  // pivot).
  // The delta is concentrated in the few below-pivot blocks (dense contracts) to keep it large
  // while
  // the pivots stay low.
  private static final int HEAVY_UP_TO_HEIGHT = 10; // = pivot_A
  // This state size is a balance, not a "bigger is safer" knob. The throttled world-state DOWNLOAD
  // must still be running when the pivot switches (~0.35s after Stage 1) so recovery fires — but
  // the
  // post-recovery world-state HEAL (fork A's storage reconciled to fork B's) runs under the same
  // trie-node/hash throttle, so an oversized delta makes the heal exceed the phase-3 timeout. 30 x
  // 24 = 720 fresh slots per heavy block keeps both sides comfortable.
  private static final int CONTRACTS_PER_HEAVY_BLOCK = 30;
  // NOTE: must stay <= 256. The init code in storageContractInitCode() addresses each slot with a
  // single-byte PUSH1 operand; a larger value would overflow one byte and produce malformed
  // bytecode.
  private static final int STORAGE_SLOTS_PER_CONTRACT = 24;
  // Per-deploy gas limit: must exceed STORAGE_SLOTS_PER_CONTRACT * 22_100 (cold SSTORE) plus
  // init-code/deploy overhead, or the deploys run out of gas and write no storage at all. Also,
  // CONTRACTS_PER_HEAVY_BLOCK * DEPLOY_GAS_LIMIT must fit under the 30M block gas limit.
  private static final long DEPLOY_GAS_LIMIT = 1_000_000L;
  private static final long CHAIN_ID = 1L;

  // Block-build retry bounds (see buildBlock): each attempt runs a fresh build with a longer
  // window, so a heavily loaded miner still gets time to pack all the storage-heavy deploys.
  private static final int MAX_BUILD_ATTEMPTS = 10;
  private static final long MAX_BUILD_WINDOW_MILLIS = 8_000L;
  // Empty blocks are ready almost immediately, so they always use this short, fixed window.
  private static final long EMPTY_BUILD_WINDOW_MILLIS = 150L;
  // Starting window for transaction-bearing blocks, before any adaptive growth (see
  // transactionBlockBuildWindowMillis).
  private static final long INITIAL_BUILD_WINDOW_MILLIS = 500L;

  private static final String FEE_RECIPIENT_A = "0x1111111111111111111111111111111111111111";
  private static final String FEE_RECIPIENT_B = "0x2222222222222222222222222222222222222222";

  private final OkHttpClient httpClient = new OkHttpClient();
  private final ObjectMapper mapper = new ObjectMapper();

  private Cluster noDiscoveryCluster;

  // Fork B's execution payloads, kept so we can pre-cache their headers on the sync node.
  private final List<ObjectNode> forkBBlocks = new java.util.ArrayList<>();

  // Benefactor nonce for the fork-A deploy transactions.
  private long nonceForkA = 0;

  // Adaptive build window for transaction-bearing blocks: remembered across blocks so that once one
  // heavy block proves a longer window is needed on this (possibly loaded) machine, later heavy
  // blocks start from that proven duration instead of re-climbing from the minimum each time.
  private long transactionBlockBuildWindowMillis = INITIAL_BUILD_WINDOW_MILLIS;

  @Test
  public void recoversAndSyncsToCompetingForkAfterPivotReorg() throws Exception {
    final String genesis = buildMergeAtGenesis();

    noDiscoveryCluster =
        new Cluster(new ClusterConfigurationBuilder().awaitPeerDiscovery(false).build(), net);

    final BesuNode minerA = createMiner("minerA", genesis);
    final BesuNode minerB = createMiner("minerB", genesis);
    final BesuNode syncNode = createSyncNode("syncNode", genesis);

    noDiscoveryCluster.start(minerA, minerB, syncNode);

    // Common prefix: build block 1 on A (empty) and import it into B so both forks share it.
    final ObjectNode commonBlock = buildBlock(minerA, FEE_RECIPIENT_A, 0);
    importBlock(minerB, commonBlock);
    minerA.verify(blockchain.currentHeight(COMMON_HEIGHT));
    minerB.verify(blockchain.currentHeight(COMMON_HEIGHT));

    // Fork A on minerA, fork B on minerB. Distinct feeRecipient => divergent block 2. Fork A's
    // blocks below its pivot are heavy so its Stage-2 body/receipt download is long; fork B stays
    // empty (its blocks still differ from fork A via the feeRecipient, which is all the divergence
    // the recovery needs).
    ObjectNode forkAHead = null;
    for (int height = COMMON_HEIGHT + 1; height <= FORK_A_HEIGHT; height++) {
      final int contracts = height <= HEAVY_UP_TO_HEIGHT ? CONTRACTS_PER_HEAVY_BLOCK : 0;
      submitStorageDeploys(minerA, contracts);
      forkAHead = buildBlock(minerA, FEE_RECIPIENT_A, contracts);
    }
    minerA.verify(blockchain.currentHeight(FORK_A_HEIGHT));

    ObjectNode forkBHead = null;
    for (int height = COMMON_HEIGHT + 1; height <= FORK_B_HEIGHT; height++) {
      forkBHead = buildBlock(minerB, FEE_RECIPIENT_B, 0);
      forkBBlocks.add(forkBHead);
    }
    minerB.verify(blockchain.currentHeight(FORK_B_HEIGHT));

    final String forkAHeadHash = forkAHead.get("blockHash").asText();
    final String forkBHeadHash = forkBHead.get("blockHash").asText();

    // Pre-cache every fork-B header on the sync node so its pivot selector can resolve fork B's
    // pivot instantly from cache (no peer round-trip). Inert until we point the CL at fork B.
    for (final ObjectNode forkBBlock : forkBBlocks) {
      sendNewPayload(syncNode, forkBBlock);
    }

    // Capture console now so we only accumulate sync-phase logs (only the sync node emits the
    // BackwardHeaderDriver / SnapSyncChainDownloader lines we key off of).
    noDiscoveryCluster.startConsoleCapture();

    // Phase 1: snap-sync the sync node toward fork A, connected only to miner A.
    syncNode.execute(adminTransactions.addPeer(minerA.enodeUrl()));
    syncNode.verify(net.awaitPeerCount(1));

    sendNewPayload(syncNode, forkAHead);
    // Resend the fork-A FCU (fast poll) until Stage 1 (backward header download) for the fork-A
    // pivot completes. Fork A's Stage-2 (body/receipt) download of the heavy blocks below the pivot
    // then begins and imports many blocks over the next few seconds.
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

    // Phase 2: fork A's Stage-1 header round is done (downloaded from A only, uncorrupted). Connect
    // miner A to miner B and point A's forkchoice at fork B's head: A backward-syncs fork B from B
    // and reorgs from fork A onto fork B, so it now serves fork B's canonical chain to the sync
    // node. The sync node keeps its single peer (A), which flips forks — no competing-peer header
    // corruption. Point the sync node's CL at fork B too; its cached fork-B headers let the
    // frequently-rechecked pivot advance onto fork B (a continuation, anchor = the fork-A pivot)
    // during round 1's long storage-heavy world-state download, and the continuation round then
    // downloads fork B's headers from A → anchor mismatch → recovery.
    minerA.execute(adminTransactions.addPeer(minerB.enodeUrl()));
    fcuHeadOnly(minerA, forkBHeadHash);
    fcuHeadOnly(syncNode, forkBHeadHash);

    // Keep nudging A onto fork B (its backward-sync + reorg takes a moment) and the sync node's CL
    // at fork B until the continuation Stage-1 round hits the anchor mismatch -> recovery WARN.
    await()
        .atMost(Duration.ofMinutes(4))
        .pollInterval(Duration.ofMillis(250))
        .until(
            () -> {
              fcuHeadOnly(minerA, forkBHeadHash);
              fcuHeadOnly(syncNode, forkBHeadHash);
              return noDiscoveryCluster.peekConsoleContents().contains("Anchor mismatch at #");
            });

    // Recovery succeeded: the sync node fully adopts fork B (head number and hash match). After
    // recovery the world-state download briefly dead-ends on the old pivot root and re-pivots once
    // the (lowered) stall detector trips (~10-15s); 3 min leaves ample headroom for that.
    await()
        .atMost(Duration.ofMinutes(3))
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

    final EthBlock.Block syncedHead =
        syncNode.execute(
            ethTransactions.block(
                DefaultBlockParameter.valueOf(BigInteger.valueOf(FORK_B_HEIGHT))));
    assertThat(syncedHead.getHash()).isEqualTo(forkBHeadHash);
  }

  /**
   * Snap sync persists its chain-sync state and resumes after a restart. The sync node is stopped
   * mid-sync — while the (throttled) world-state download is still running, so the two-stage
   * download has not finished and {@code <data-path>/syncFolder/chain-sync-state.rlp} has not been
   * deleted. It is then restarted against the same data directory: {@code
   * SnapSyncChainDownloader.initializeChainSyncState} reloads the persisted state via {@code
   * handleLoadedState}, snap re-engages for the same pivot, and the node syncs fully to the head.
   *
   * <p>This exercises the persist → reload → resume path end-to-end through a real process restart
   * and the real on-disk state file — coverage the {@code
   * SnapSyncChainDownloaderHandleLoadedStateTest} unit tests (which construct the persisted state
   * directly) cannot give. We stop promptly once Stage 1's headers are imported — well before the
   * whole two-stage download finishes and deletes the state file — so a persisted state is reliably
   * present to reload.
   *
   * <p>Note on which {@code handleLoadedState} branch runs: the stop fires right when Stage 1's
   * headers finish importing, which is slightly before {@code headersDownloadComplete=true} is
   * persisted, so the reloaded state has {@code headersDownloadComplete=false} and the restart
   * re-runs Stage 1 before finishing. The "headers complete + pivot canonical → skip Stage 1"
   * branch is real — once Stage 1 persists completion the pivot is canonical and a reload would
   * skip Stage 1 — but it is not asserted here: for a low pivot the whole snap completes within
   * ~1s, so the window between persisting completion and deleting the state file on full completion
   * is only tens of milliseconds, too small to land in reliably by stop/restart. That branch stays
   * covered by the {@code SnapSyncChainDownloaderHandleLoadedStateTest} unit tests.
   */
  @Test
  public void resumesSnapSyncFromPersistedStateAfterRestart() throws Exception {
    final String genesis = buildMergeAtGenesis();

    noDiscoveryCluster =
        new Cluster(new ClusterConfigurationBuilder().awaitPeerDiscovery(false).build(), net);

    final BesuNode miner = createMiner("miner", genesis);
    final BesuNode syncNode = createSyncNode("syncNode", genesis);

    noDiscoveryCluster.start(miner, syncNode);

    // A single chain: heavy storage-writing blocks below the pivot (= head - 64 = 10) make the
    // sync node's throttled world-state download run for several seconds — the window in which we
    // stop it after Stage 1 has completed but before the sync finishes.
    ObjectNode head = null;
    for (int height = COMMON_HEIGHT; height <= FORK_A_HEIGHT; height++) {
      final int contracts = height <= HEAVY_UP_TO_HEIGHT ? CONTRACTS_PER_HEAVY_BLOCK : 0;
      submitStorageDeploys(miner, contracts);
      head = buildBlock(miner, FEE_RECIPIENT_A, contracts);
    }
    miner.verify(blockchain.currentHeight(FORK_A_HEIGHT));
    final String headHash = head.get("blockHash").asText();

    // Start snap-syncing toward the miner.
    noDiscoveryCluster.startConsoleCapture();
    syncNode.execute(adminTransactions.addPeer(miner.enodeUrl()));
    syncNode.verify(net.awaitPeerCount(1));
    sendNewPayload(syncNode, head);

    // Drive it until Stage 1's headers have been imported, then stop promptly. This reliably
    // catches the node mid-sync with a persisted chain-sync-state that has not been deleted (the
    // state file is deleted only when the whole two-stage download finishes; see the note above).
    await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofMillis(50))
        .until(
            () -> {
              fcuHeadOnly(syncNode, headHash);
              return noDiscoveryCluster
                  .peekConsoleContents()
                  .contains("Header import progress 100.00%");
            });

    // Stop the sync node: the process is killed but its data directory — including
    // chain-sync-state.rlp — is left intact (only close(), not stop(), wipes the data dir).
    noDiscoveryCluster.stopNode(syncNode);

    // Re-capture so the console below reflects only the restarted process.
    noDiscoveryCluster.startConsoleCapture();

    // Restart against the same data directory and re-establish the peer + consensus head (runtime
    // peering and the newPayload header cache do not survive the restart).
    noDiscoveryCluster.startNode(syncNode);
    syncNode.execute(adminTransactions.addPeer(miner.enodeUrl()));
    syncNode.verify(net.awaitPeerCount(1));
    sendNewPayload(syncNode, head);

    // The reloaded state lets the resume skip Stage 1 and finish the world-state download, syncing
    // fully to the head.
    await()
        .atMost(Duration.ofMinutes(4))
        .pollInterval(Duration.ofSeconds(1))
        .until(
            () -> {
              fcuHeadOnly(syncNode, headHash);
              final EthBlock.Block h =
                  syncNode.execute(
                      ethTransactions.block(
                          DefaultBlockParameter.valueOf(BigInteger.valueOf(FORK_A_HEIGHT))));
              return h != null && headHash.equals(h.getHash());
            });

    final EthBlock.Block syncedHead =
        syncNode.execute(
            ethTransactions.block(
                DefaultBlockParameter.valueOf(BigInteger.valueOf(FORK_A_HEIGHT))));
    assertThat(syncedHead.getHash()).isEqualTo(headHash);

    // Prove it resumed from the persisted state rather than starting a fresh snap sync: snap
    // re-engaged ("Starting two-stage fast sync chain download") but did NOT create a new initial
    // state ("Created initial chain sync state" is logged only on the fresh, no-loaded-state path).
    final String restartConsole = noDiscoveryCluster.peekConsoleContents();
    assertThat(restartConsole).contains("Starting two-stage fast sync chain download");
    assertThat(restartConsole).doesNotContain("Created initial chain sync state");
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
                    .miningEnabled());
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
                    // Re-check the snap pivot very frequently (default is once per minute, far
                    // longer than this whole test) so the pivot can actually be re-evaluated and
                    // switched to fork B while round 1's world-state download is still running.
                    // Also throttle the world-state download to one request/item at a time so that
                    // download runs long enough for a re-check to fire during it.
                    //
                    // Lower the world-state stall thresholds too. After recovery switches to fork
                    // B,
                    // the world-state download briefly dead-ends on the old (fork-A) pivot root the
                    // peer no longer serves; it recovers when the stall detector trips and
                    // re-pivots.
                    // With the defaults (1000 no-progress requests AND 5 min) that dead-end takes
                    // ~5
                    // minutes to clear under this throttle. Tripping after 50 requests / 10s keeps
                    // the
                    // test fast without spurious stalls: every throttled request in the normal
                    // phases
                    // makes progress (resetting the counter), so only the genuine dead-end
                    // accumulates.
                    .extraCLIOptions(
                        List.of(
                            "--Xsnapsync-synchronizer-pivot-block-check-interval-millis=100",
                            "--Xsynchronizer-world-state-request-parallelism=1",
                            "--Xsynchronizer-world-state-hash-count-per-request=1",
                            "--Xsnapsync-synchronizer-storage-count-per-request=1",
                            "--Xsnapsync-synchronizer-bytecode-count-per-request=1",
                            "--Xsnapsync-synchronizer-trienode-count-per-request=1",
                            "--Xsnapsync-synchronizer-flat-account-healed-count-per-request=1",
                            "--Xsnapsync-synchronizer-flat-slot-healed-count-per-request=1",
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
   * Submits {@code count} contract-creation transactions to the miner's pool. Each deploys a tiny
   * contract whose constructor writes {@link #STORAGE_SLOTS_PER_CONTRACT} storage slots, so every
   * deployed account has its own storage trie — a storage-heavy, throttleable world-state delta.
   */
  private void submitStorageDeploys(final BesuNode miner, final int count) {
    final Credentials benefactor = Credentials.create(Accounts.GENESIS_ACCOUNT_ONE_PRIVATE_KEY);
    final String initCode = storageContractInitCode();
    for (int i = 0; i < count; i++) {
      final RawTransaction tx =
          RawTransaction.createContractTransaction(
              BigInteger.valueOf(nonceForkA++),
              BigInteger.valueOf(1_000), // gas price (wei), above the base fee
              BigInteger.valueOf(DEPLOY_GAS_LIMIT), // enough for the SSTOREs + deploy
              BigInteger.ZERO,
              initCode);
      final String signed =
          Numeric.toHexString(TransactionEncoder.signMessage(tx, CHAIN_ID, benefactor));
      miner.execute(ethTransactions.sendRawTransaction(signed));
    }
  }

  /**
   * EVM init code that writes {@link #STORAGE_SLOTS_PER_CONTRACT} storage slots (slot j = 1) in the
   * constructor, then returns a 1-byte runtime. Unrolled (no loop) to keep the bytecode trivial.
   */
  private static String storageContractInitCode() {
    final StringBuilder code = new StringBuilder("0x");
    for (int j = 0; j < STORAGE_SLOTS_PER_CONTRACT; j++) {
      // PUSH1 1 ; PUSH1 j ; SSTORE. The PUSH1 operand is a single byte, so the slot index j must
      // fit in one byte (see the STORAGE_SLOTS_PER_CONTRACT <= 256 note at its declaration).
      code.append("6001").append("60").append(String.format("%02x", j)).append("55");
    }
    // PUSH1 0 ; PUSH1 0 ; MSTORE ; PUSH1 1 ; PUSH1 31 ; RETURN  -> return a 1-byte (0x00) runtime
    code.append("60006000526001601ff3");
    return code.toString();
  }

  /**
   * Drives a single PoS block build on {@code miner} over the Engine API (V1) and returns the
   * execution payload. Any pending pooled transactions are included in the block.
   */
  private ObjectNode buildBlock(
      final BesuNode miner, final String feeRecipient, final int expectedTxCount)
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
    for (int attempt = 0; attempt < MAX_BUILD_ATTEMPTS; attempt++) {
      // Distinct timestamp per attempt => distinct payload id => a fresh, uncancelled build over
      // the full transaction pool, rather than the deduplicated, already-finalized previous build.
      final String payloadId =
          startBlockBuild(miner, headHash, baseTimestamp + attempt, feeRecipient);
      sleep(buildWindowMillis);
      final ObjectNode payload = fetchPayload(miner, payloadId);
      if (payload.get("transactions").size() == expectedTxCount) {
        executionPayload = payload;
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

    importBlock(miner, executionPayload);
    return executionPayload;
  }

  /** engine_forkchoiceUpdatedV1 with payload attributes; returns the payload id for the build. */
  private String startBlockBuild(
      final BesuNode miner, final String headHash, final long timestamp, final String feeRecipient)
      throws IOException {
    final String fcuWithAttributes =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV1\",\"params\":["
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
            + "\"}],\"id\":67}";
    try (Response response = engineCall(miner, fcuWithAttributes).execute()) {
      assertThat(response.code()).isEqualTo(200);
      final String payloadId = result(response).get("payloadId").asText();
      assertThat(payloadId).isNotEmpty();
      return payloadId;
    }
  }

  /** engine_getPayloadV1 for the given payload id; returns the execution payload. */
  private ObjectNode fetchPayload(final BesuNode miner, final String payloadId) throws IOException {
    final String getPayload =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_getPayloadV1\",\"params\":[\""
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

  /** engine_newPayloadV1 (VALID) + engine_forkchoiceUpdatedV1 to make the block canonical. */
  private void importBlock(final BesuNode node, final ObjectNode executionPayload)
      throws IOException {
    final String newPayload =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_newPayloadV1\",\"params\":["
            + executionPayload
            + "],\"id\":67}";
    try (Response response = engineCall(node, newPayload).execute()) {
      assertThat(response.code()).isEqualTo(200);
      assertThat(result(response).get("status").asText()).isEqualTo("VALID");
    }
    final String blockHash = executionPayload.get("blockHash").asText();
    final String fcu =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV1\",\"params\":["
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
  private void sendNewPayload(final BesuNode node, final ObjectNode executionPayload)
      throws IOException {
    final String newPayload =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_newPayloadV1\",\"params\":["
            + executionPayload
            + "],\"id\":67}";
    try (Response response = engineCall(node, newPayload).execute()) {
      assertThat(response.code()).isEqualTo(200);
    }
  }

  /** forkchoiceUpdatedV1 with head only (no safe/finalized, no attributes): pure non-finality. */
  private void fcuHeadOnly(final BesuNode node, final String headHash) throws IOException {
    final String fcu =
        "{\"jsonrpc\":\"2.0\",\"method\":\"engine_forkchoiceUpdatedV1\",\"params\":["
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

  /**
   * Builds a merged-at-genesis (TTD=0, pre-Shanghai so Engine V1 applies) genesis with a single
   * prefunded benefactor. The snap-syncable state comes from transactions below the pivot, not from
   * the genesis alloc (the sync node already holds the genesis state locally).
   */
  private static String buildMergeAtGenesis() {
    return "{\"config\":{"
        + "\"chainId\":1,\"homesteadBlock\":0,\"eip150Block\":0,\"eip155Block\":0,\"eip158Block\":0,"
        + "\"byzantiumBlock\":0,\"constantinopleBlock\":0,\"petersburgBlock\":0,\"istanbulBlock\":0,"
        + "\"muirGlacierBlock\":0,\"berlinBlock\":0,\"londonBlock\":0,\"terminalTotalDifficulty\":0,"
        + "\"ethash\":{}},"
        + "\"nonce\":\"0x42\",\"timestamp\":\"0x0\","
        + "\"extraData\":\"0x0000000000000000000000000000000000000000000000000000000000000000\","
        + "\"gasLimit\":\"0x1C9C380\",\"difficulty\":\"0x400000000\","
        + "\"mixHash\":\""
        + ZERO_HASH
        + "\",\"coinbase\":\"0x0000000000000000000000000000000000000000\","
        + "\"alloc\":{"
        + "\"fe3b557e8fb62b89f4916b721be55ceb828dbd73\":{\"balance\":\"0x200000000000000000000000\"}"
        + "},"
        + "\"number\":\"0x0\",\"gasUsed\":\"0x0\",\"parentHash\":\""
        + ZERO_HASH
        + "\",\"baseFeePerGas\":\"0x7\"}";
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
