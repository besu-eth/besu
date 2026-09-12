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
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.TransactionEncoder;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.utils.Numeric;

/**
 * Shared infrastructure for the snap/2 end-to-end tests: miners serving snap/2 (BALs and state
 * ranges), a throttled snap/2 sync node whose world-state download stays in flight long enough for
 * the pivot to switch mid-download, log-awaiting helpers that keep nudging the nodes with fresh
 * FCUs, and world-state assertion helpers. Subclasses provide only their scenario: fork geometry,
 * scenario transactions, and the per-category assertions.
 */
public abstract class AbstractSnapV2AcceptanceTest extends AcceptanceTestBase {

  protected static final long CHAIN_ID = 1L;
  protected static final BigInteger GAS_PRICE = BigInteger.valueOf(1_000); // above the base fee

  // A 2-slot deploy measures ~440K gas under Amsterdam state-growth pricing; 24 slots need ~2.7M.
  protected static final long DEPLOY_GAS_LIMIT = 600_000L;
  protected static final long SPLIT_DEPLOY_GAS_LIMIT = 4_000_000L;
  // A bare 21k transfer to a fresh address halts: the new-account state-growth charge is drawn
  // from the same gas pool.
  protected static final long TRANSFER_GAS_LIMIT = 250_000L;

  protected static final BigInteger TRANSFER_WEI = BigInteger.TEN.pow(18);

  // The pivot is reused while the head stays within this window of the last pivot, and refreshed
  // past it.
  protected static final int PIVOT_BLOCK_WINDOW_VALIDITY = 10;

  protected static final Address BENEFACTOR_ADDRESS =
      Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  protected static final Credentials BENEFACTOR =
      Credentials.create(Accounts.GENESIS_ACCOUNT_ONE_PRIVATE_KEY);

  protected final AmsterdamEngineApi engineApi = new AmsterdamEngineApi(ethTransactions);

  protected Cluster noDiscoveryCluster;
  protected BesuNode syncNode;

  protected void startCluster(final BesuNode... nodes) throws IOException {
    noDiscoveryCluster =
        new Cluster(new ClusterConfigurationBuilder().awaitPeerDiscovery(false).build(), net);
    noDiscoveryCluster.start(nodes);
  }

  /** A miner serving snap/2 (BALs and state ranges) to the sync node. */
  protected BesuNode createMiner(final String name, final String genesis) throws IOException {
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
                            "--Xsnap2-enabled",
                            // the heavy blocks carry 1000 txs from the single benefactor
                            "--tx-pool-max-future-by-sender=5000")));
    node.setSynchronizerConfiguration(
        SynchronizerConfiguration.builder()
            .syncMode(SyncMode.FULL)
            .syncMinimumPeerCount(1)
            .snapSyncConfiguration(
                ImmutableSnapSyncConfiguration.builder().isSnapServerEnabled(true).build())
            .build());
    return node;
  }

  /**
   * A snap/2 downloader. The pivot check interval is shortened (default 1 minute) and the
   * world-state download throttled to one item per request, so the pivot is re-evaluated, and
   * advanced, mid-download. The lowered stall thresholds clear the brief dead-end after the
   * retarget in seconds.
   */
  protected BesuNode createSyncNode(final String name, final String genesis) throws IOException {
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
   * Repeats {@code nudge} (the nodes need fresh FCUs to make progress) until the sync console shows
   * {@code logLine}; dumps the console on timeout to aid CI debugging.
   */
  protected void awaitLog(
      final String logLine,
      final Duration timeout,
      final Duration pollInterval,
      final EngineNudge nudge) {
    try {
      await()
          .atMost(timeout)
          .pollInterval(pollInterval)
          .until(
              () -> {
                nudge.run();
                return noDiscoveryCluster.peekConsoleContents().contains(logLine);
              });
    } catch (final Throwable t) {
      printConsole("TIMED OUT waiting for '" + logLine + "'");
      throw t;
    }
  }

  /** Same as above, nudging the sync node with a fresh FCU to {@code nudgeHeadHash}. */
  protected void awaitLog(
      final String logLine,
      final Duration timeout,
      final Duration pollInterval,
      final String nudgeHeadHash) {
    awaitLog(logLine, timeout, pollInterval, () -> engineApi.setHead(syncNode, nudgeHeadHash));
  }

  @FunctionalInterface
  protected interface EngineNudge {
    void run() throws IOException;
  }

  /** Waits until the sync node's block at {@code height} is {@code headHash}. */
  protected void awaitHead(final String headHash, final long height) {
    try {
      await()
          .atMost(Duration.ofMinutes(8))
          .pollInterval(Duration.ofSeconds(2))
          .until(
              () -> {
                engineApi.setHead(syncNode, headHash);
                final EthBlock.Block head = blockAt(syncNode, height);
                return head != null && headHash.equals(head.getHash());
              });
    } catch (final Throwable t) {
      printConsole("SYNC COMPLETION TIMED OUT");
      throw t;
    }
  }

  protected void printConsole(final String header) {
    System.out.println(
        header + " - sync console so far:\n" + noDiscoveryCluster.peekConsoleContents());
  }

  protected EthBlock.Block blockAt(final BesuNode node, final long height) {
    return node.execute(
        ethTransactions.block(DefaultBlockParameter.valueOf(BigInteger.valueOf(height))));
  }

  protected Account accountAt(final Address address) {
    return Account.create(ethTransactions, address);
  }

  /** The contract deployed by the benefactor at the given nonce. */
  protected Account contractAt(final long deployNonce) {
    return accountAt(Address.contractAddress(BENEFACTOR_ADDRESS, deployNonce));
  }

  protected void assertStorage(final Account contract, final int slot, final int expectedValue) {
    assertThat(syncNode.execute(ethTransactions.getStorageAt(contract, BigInteger.valueOf(slot))))
        .isEqualTo(storageValueHex(expectedValue));
  }

  protected void assertBalance(final String address, final BigInteger expectedWei) {
    assertThat(
            syncNode.execute(ethTransactions.getBalance(accountAt(Address.fromHexString(address)))))
        .isEqualTo(expectedWei);
  }

  protected static String storageValueHex(final int value) {
    return "0x" + wordHex(value);
  }

  protected static String wordHex(final int value) {
    return String.format("%064x", value);
  }

  protected void sendRaw(
      final BesuNode node, final RawTransaction tx, final Credentials credentials) {
    node.execute(
        ethTransactions.sendRawTransaction(
            Numeric.toHexString(TransactionEncoder.signMessage(tx, CHAIN_ID, credentials))));
  }

  /**
   * Amsterdam active at genesis (every block carries a BAL), a prefunded benefactor, and the Prague
   * system contracts that block building needs.
   */
  protected static String loadAmsterdamGenesis() {
    try (var in =
        AbstractSnapV2AcceptanceTest.class.getResourceAsStream(
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
