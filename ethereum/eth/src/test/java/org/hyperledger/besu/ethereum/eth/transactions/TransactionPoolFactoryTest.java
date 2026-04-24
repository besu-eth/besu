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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LAYERED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LEGACY;
import static org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule.DEFAULT_CHAIN_ID;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.GasPricePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;
import org.hyperledger.besu.testutil.TestClock;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TransactionPoolFactoryTest {
  @Mock ProtocolSchedule schedule;
  @Mock ProtocolContext context;
  @Mock ProtocolSpec protocolSpec;
  @Mock MutableBlockchain blockchain;
  @Mock EthContext ethContext;
  @Mock EthMessages ethMessages;
  @Mock EthScheduler ethScheduler;
  @Mock PeerTransactionTracker peerTransactionTracker;
  @Mock TransactionsMessageSender transactionsMessageSender;
  @Mock NewPooledTransactionHashesMessageSender newPooledTransactionHashesMessageSender;

  TransactionPool pool;
  EthPeers ethPeers;

  SyncState syncState;

  EthProtocolManager ethProtocolManager;

  ProtocolContext protocolContext;

  @BeforeEach
  public void setup() {
    when(blockchain.getBlockHashByNumber(anyLong())).thenReturn(Optional.of(mock(Hash.class)));
    final Block mockBlock = mock(Block.class);
    when(mockBlock.getHash()).thenReturn(Hash.ZERO);
    when(blockchain.getGenesisBlock()).thenReturn(mockBlock);
    when(context.getBlockchain()).thenReturn(blockchain);

    final NodeMessagePermissioningProvider nmpp = (destinationEnode, code) -> true;
    ethPeers =
        new EthPeers(
            () -> protocolSpec,
            TestClock.fixed(),
            new NoOpMetricsSystem(),
            EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
            Collections.singletonList(nmpp),
            Bytes.random(64),
            25,
            25,
            false,
            SyncMode.SNAP,
            new ForkIdManager(blockchain, Collections.emptyList(), Collections.emptyList()));
    when(ethContext.getEthMessages()).thenReturn(ethMessages);
    when(ethContext.getEthPeers()).thenReturn(ethPeers);

    when(ethContext.getScheduler()).thenReturn(ethScheduler);
  }

  @Test
  public void notRegisteredToBlockAddedEventBeforeInitialSyncIsDone() {
    setupInitialSyncPhase(true);
    ArgumentCaptor<BlockAddedObserver> blockAddedListeners =
        ArgumentCaptor.forClass(BlockAddedObserver.class);
    verify(blockchain, atLeastOnce()).observeBlockAdded(blockAddedListeners.capture());

    assertThat(pool.isEnabled()).isFalse();
  }

  @Test
  public void assertPoolDisabledIfChainInSyncWithoutInitialSync() {
    SyncState syncSpy = spy(new SyncState(blockchain, ethPeers, true, Optional.empty()));
    ArgumentCaptor<Synchronizer.InSyncListener> chainSyncCaptor =
        ArgumentCaptor.forClass(Synchronizer.InSyncListener.class);

    setupInitialSyncPhase(syncSpy);
    // verify that we are registered to the sync state
    verify(syncSpy).subscribeInSync(chainSyncCaptor.capture());
    // Retrieve the captured InSyncListener
    Synchronizer.InSyncListener chainSyncListener = chainSyncCaptor.getValue();

    // mock chain being in sync:
    chainSyncListener.onInSyncStatusChange(true);

    // assert pool is disabled if chain in sync and initial sync not done
    assertThat(pool.isEnabled()).isFalse();

    // mock initial sync done (avoid triggering initial sync listener)
    when(syncSpy.isInitialSyncPhaseDone()).thenReturn(true);

    // assert pool is enabled when chain in sync and initial sync done
    chainSyncListener.onInSyncStatusChange(true);
    assertThat(pool.isEnabled()).isTrue();

    // assert pool is re-disabled when initial sync is incomplete but chain reaches head:
    when(syncSpy.isInitialSyncPhaseDone()).thenCallRealMethod();
    chainSyncListener.onInSyncStatusChange(false);
    assertThat(pool.isEnabled()).isFalse();
  }

  @Test
  public void registeredToBlockAddedEventAfterInitialSyncIsDone() {
    setupInitialSyncPhase(true);
    syncState.markInitialSyncPhaseAsDone();

    ArgumentCaptor<BlockAddedObserver> blockAddedListeners =
        ArgumentCaptor.forClass(BlockAddedObserver.class);
    verify(blockchain, atLeastOnce()).observeBlockAdded(blockAddedListeners.capture());

    assertThat(blockAddedListeners.getAllValues()).contains(pool);
    assertThat(pool.isEnabled()).isTrue();
  }

  @Test
  public void registeredToBlockAddedEventIfNoInitialSync() {
    setupInitialSyncPhase(false);

    ArgumentCaptor<BlockAddedObserver> blockAddedListeners =
        ArgumentCaptor.forClass(BlockAddedObserver.class);
    verify(blockchain, atLeastOnce()).observeBlockAdded(blockAddedListeners.capture());

    assertThat(blockAddedListeners.getAllValues()).contains(pool);
    assertThat(pool.isEnabled()).isTrue();
  }

  @Test
  public void incomingTransactionMessageHandlersDisabledBeforeInitialSyncIsDone() {
    setupInitialSyncPhase(true);
    ArgumentCaptor<EthMessages.MessageCallback> messageHandlers =
        ArgumentCaptor.forClass(EthMessages.MessageCallback.class);
    verify(ethMessages, atLeast(2)).subscribe(anyInt(), messageHandlers.capture());

    assertThat(messageHandlers.getAllValues())
        .haveAtLeastOne(
            new Condition<>(
                h ->
                    h instanceof NewPooledTransactionHashesMessageHandler
                        && !((NewPooledTransactionHashesMessageHandler) h).isEnabled(),
                "pooled transaction hashes handler should be disabled"));

    assertThat(messageHandlers.getAllValues())
        .haveAtLeastOne(
            new Condition<>(
                h ->
                    h instanceof TransactionsMessageHandler
                        && !((TransactionsMessageHandler) h).isEnabled(),
                "transaction messages handler should be disabled"));
  }

  @Test
  public void incomingTransactionMessageHandlersRegisteredAfterInitialSyncIsDone() {
    setupInitialSyncPhase(true);
    syncState.markInitialSyncPhaseAsDone();

    ArgumentCaptor<EthMessages.MessageCallback> messageHandlers =
        ArgumentCaptor.forClass(EthMessages.MessageCallback.class);
    verify(ethMessages, atLeast(2)).subscribe(anyInt(), messageHandlers.capture());

    assertThat(messageHandlers.getAllValues())
        .haveAtLeastOne(
            new Condition<>(
                h ->
                    h instanceof NewPooledTransactionHashesMessageHandler
                        && ((NewPooledTransactionHashesMessageHandler) h).isEnabled(),
                "pooled transaction hashes handler should be enabled"));

    assertThat(messageHandlers.getAllValues())
        .haveAtLeastOne(
            new Condition<>(
                h ->
                    h instanceof TransactionsMessageHandler
                        && ((TransactionsMessageHandler) h).isEnabled(),
                "transaction messages handler should be enabled"));
  }

  @Test
  public void incomingTransactionMessageHandlersRegisteredIfNoInitialSync() {
    setupInitialSyncPhase(false);

    ArgumentCaptor<EthMessages.MessageCallback> messageHandlers =
        ArgumentCaptor.forClass(EthMessages.MessageCallback.class);
    verify(ethMessages, atLeast(0)).subscribe(anyInt(), messageHandlers.capture());

    assertThat(messageHandlers.getAllValues())
        .haveAtLeastOne(
            new Condition<>(
                h ->
                    h instanceof NewPooledTransactionHashesMessageHandler
                        && ((NewPooledTransactionHashesMessageHandler) h).isEnabled(),
                "pooled transaction hashes handler should be enabled"));

    assertThat(messageHandlers.getAllValues())
        .haveAtLeastOne(
            new Condition<>(
                h ->
                    h instanceof TransactionsMessageHandler
                        && ((TransactionsMessageHandler) h).isEnabled(),
                "transaction messages handler should be enabled"));
  }

  @Test
  public void txPoolStartsDisabledWhenInitialSyncPhaseIsRequired() {
    // when using any of the initial syncs (SNAP, FAST, ...), txpool starts disabled
    // and is enabled only after it is in sync
    setupInitialSyncPhase(true);
    assertThat(pool.isEnabled()).isFalse();
  }

  @Test
  public void callingGetNextNonceForSenderOnDisabledTxPoolWorks() {
    setupInitialSyncPhase(true);

    assertThat(pool.getNextNonceForSender(Address.fromHexString("0x123abc"))).isEmpty();
  }

  @Test
  public void txPoolStartsEnabledWhenFullSyncConfigured() {
    // when using FULL sync, txpool starts enabled, because it could be
    // that it is the first node on a new network and so, it is in sync with genesis
    // and must accept txs. Otherwise, asap it find a sync target that is ahead, the txpool
    // will be disabled, until in sync.
    setupInitialSyncPhase(false);
    assertThat(pool.isEnabled()).isTrue();
  }

  private void setupInitialSyncPhase(final boolean hasInitialSyncPhase) {
    syncState = new SyncState(blockchain, ethPeers, hasInitialSyncPhase, Optional.empty());
    setupInitialSyncPhase(syncState);
  }

  private void setupInitialSyncPhase(final SyncState syncState) {
    pool = createTransactionPool(LAYERED, syncState);
    SynchronizerConfiguration syncConfig = mock(SynchronizerConfiguration.class);
    when(syncConfig.getSyncMode()).thenReturn(SyncMode.FULL);
    ethProtocolManager =
        new EthProtocolManager(
            blockchain,
            BigInteger.ONE,
            mock(WorldStateArchive.class),
            pool,
            EthProtocolConfiguration.DEFAULT,
            ethPeers,
            mock(EthMessages.class),
            ethContext,
            Collections.emptyList(),
            Optional.empty(),
            syncConfig,
            mock(EthScheduler.class),
            mock(ForkIdManager.class));
  }

  @Test
  public void
      createLegacyTransactionPool_shouldUseBaseFeePendingTransactionsSorter_whenLondonEnabled() {
    setupScheduleWith(new StubGenesisConfigOptions().londonBlock(0));

    final TransactionPool pool = createAndEnableTransactionPool(LEGACY, syncState);

    assertThat(pool.pendingTransactionsImplementation())
        .isEqualTo(BaseFeePendingTransactionsSorter.class);
  }

  @Test
  public void
      createLegacyTransactionPool_shouldUseGasPricePendingTransactionsSorter_whenLondonNotEnabled() {
    setupScheduleWith(new StubGenesisConfigOptions().berlinBlock(0));

    final TransactionPool pool = createAndEnableTransactionPool(LEGACY, syncState);

    assertThat(pool.pendingTransactionsImplementation())
        .isEqualTo(GasPricePendingTransactionsSorter.class);
  }

  @Test
  public void
      createLayeredTransactionPool_shouldUseBaseFeePendingTransactionsSorter_whenLondonEnabled() {
    setupScheduleWith(new StubGenesisConfigOptions().londonBlock(0));

    final TransactionPool pool = createAndEnableTransactionPool(LAYERED, syncState);

    assertThat(pool.pendingTransactionsImplementation())
        .isEqualTo(LayeredPendingTransactions.class);

    assertThat(pool.logStats()).startsWith("Basefee Prioritized");
  }

  @Test
  public void
      createLayeredTransactionPool_shouldUseGasPricePendingTransactionsSorter_whenLondonNotEnabled() {
    setupScheduleWith(new StubGenesisConfigOptions().berlinBlock(0));

    final TransactionPool pool = createAndEnableTransactionPool(LAYERED, syncState);

    assertThat(pool.pendingTransactionsImplementation())
        .isEqualTo(LayeredPendingTransactions.class);

    assertThat(pool.logStats()).startsWith("GasPrice Prioritized");
  }

  private void setupScheduleWith(final StubGenesisConfigOptions config) {
    schedule =
        new ProtocolScheduleBuilder(
                config,
                Optional.of(DEFAULT_CHAIN_ID),
                ProtocolSpecAdapters.create(0, Function.identity()),
                false,
                EvmConfiguration.DEFAULT,
                MiningConfiguration.MINING_DISABLED,
                new BadBlockManager(),
                false,
                BalConfiguration.DEFAULT,
                new NoOpMetricsSystem())
            .createProtocolSchedule();

    protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadHeader()).thenReturn(new BlockHeaderTestFixture().buildHeader());

    syncState = new SyncState(blockchain, ethPeers, true, Optional.empty());
  }

  private TransactionPool createTransactionPool(
      final TransactionPoolConfiguration.Implementation implementation, final SyncState syncState) {
    return TransactionPoolFactory.createTransactionPool(
        schedule,
        context,
        ethContext,
        TestClock.fixed(),
        new TransactionPoolMetrics(new NoOpMetricsSystem()),
        syncState,
        ImmutableTransactionPoolConfiguration.builder()
            .txPoolImplementation(implementation)
            .txPoolMaxSize(1)
            .pendingTxRetentionPeriod(1)
            .unstable(
                ImmutableTransactionPoolConfiguration.Unstable.builder()
                    .txMessageKeepAliveSeconds(1)
                    .build())
            .build(),
        peerTransactionTracker,
        transactionsMessageSender,
        newPooledTransactionHashesMessageSender,
        new BlobCache(),
        MiningConfiguration.newDefault(),
        EthProtocolConfiguration.DEFAULT);
  }

  private TransactionPool createAndEnableTransactionPool(
      final TransactionPoolConfiguration.Implementation implementation, final SyncState syncState) {
    final TransactionPool txPool = createTransactionPool(implementation, syncState);
    txPool.setEnabled();
    return txPool;
  }
}
