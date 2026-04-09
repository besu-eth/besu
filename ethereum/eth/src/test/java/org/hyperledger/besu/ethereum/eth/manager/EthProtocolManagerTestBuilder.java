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
package org.hyperledger.besu.ethereum.eth.manager;

import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.TestClock;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class EthProtocolManagerTestBuilder {
  private static final BigInteger DEFAULT_NETWORK_ID = BigInteger.ONE;

  private ProtocolSchedule protocolSchedule;
  private GenesisConfig genesisConfig;
  private GenesisState genesisState;
  private Blockchain blockchain;
  private BigInteger networkId;
  private WorldStateArchive worldStateArchive;
  private TransactionPool transactionPool;
  private EthProtocolConfiguration ethereumWireProtocolConfiguration;
  private ForkIdManager forkIdManager;
  private EthPeers ethPeers;
  private EthMessages ethMessages;
  private EthMessages snapMessages;
  private EthScheduler ethScheduler;
  private EthContext ethContext;
  private List<PeerValidator> peerValidators;
  private Optional<MergePeerFilter> mergePeerFilter = Optional.empty();
  private SynchronizerConfiguration synchronizerConfiguration;
  private PeerTaskExecutor peerTaskExecutor;

  public static EthProtocolManagerTestBuilder builder() {
    return new EthProtocolManagerTestBuilder();
  }

  public EthProtocolManagerTestBuilder setProtocolSchedule(
      final ProtocolSchedule protocolSchedule) {
    this.protocolSchedule = protocolSchedule;
    return this;
  }

  public EthProtocolManagerTestBuilder setGenesisConfigFile(final GenesisConfig genesisConfig) {
    this.genesisConfig = genesisConfig;
    return this;
  }

  public EthProtocolManagerTestBuilder setGenesisState(final GenesisState genesisState) {
    this.genesisState = genesisState;
    return this;
  }

  public EthProtocolManagerTestBuilder setBlockchain(final Blockchain blockchain) {
    this.blockchain = blockchain;
    return this;
  }

  public EthProtocolManagerTestBuilder setNetworkId(final BigInteger networkId) {
    this.networkId = networkId;
    return this;
  }

  public EthProtocolManagerTestBuilder setWorldStateArchive(
      final WorldStateArchive worldStateArchive) {
    this.worldStateArchive = worldStateArchive;
    return this;
  }

  public EthProtocolManagerTestBuilder setTransactionPool(final TransactionPool transactionPool) {
    this.transactionPool = transactionPool;
    return this;
  }

  public EthProtocolManagerTestBuilder setEthereumWireProtocolConfiguration(
      final EthProtocolConfiguration ethereumWireProtocolConfiguration) {
    this.ethereumWireProtocolConfiguration = ethereumWireProtocolConfiguration;
    return this;
  }

  public EthProtocolManagerTestBuilder setForkIdManager(final ForkIdManager forkIdManager) {
    this.forkIdManager = forkIdManager;
    return this;
  }

  public EthProtocolManagerTestBuilder setEthPeers(final EthPeers ethPeers) {
    this.ethPeers = ethPeers;
    return this;
  }

  public EthProtocolManagerTestBuilder setEthMessages(final EthMessages ethMessages) {
    this.ethMessages = ethMessages;
    return this;
  }

  public EthProtocolManagerTestBuilder setSnapMessages(final EthMessages snapMessages) {
    this.snapMessages = snapMessages;
    return this;
  }

  public EthProtocolManagerTestBuilder setEthContext(final EthContext ethContext) {
    this.ethContext = ethContext;
    return this;
  }

  public EthProtocolManagerTestBuilder setPeerValidators(final List<PeerValidator> peerValidators) {
    this.peerValidators = peerValidators;
    return this;
  }

  public EthProtocolManagerTestBuilder setMergePeerFilter(
      final Optional<MergePeerFilter> mergePeerFilter) {
    this.mergePeerFilter = mergePeerFilter;
    return this;
  }

  public EthProtocolManagerTestBuilder setSynchronizerConfiguration(
      final SynchronizerConfiguration synchronizerConfiguration) {
    this.synchronizerConfiguration = synchronizerConfiguration;
    return this;
  }

  public EthProtocolManagerTestBuilder setEthScheduler(final EthScheduler ethScheduler) {
    this.ethScheduler = ethScheduler;
    return this;
  }

  public EthProtocolManagerTestBuilder setPeerTaskExecutor(
      final PeerTaskExecutor peerTaskExecutor) {
    this.peerTaskExecutor = peerTaskExecutor;
    return this;
  }

  public EthProtocolManager build() {
    if (protocolSchedule == null) {
      protocolSchedule = ProtocolScheduleFixture.TESTING_NETWORK;
    }
    if (blockchain == null) {
      if (genesisConfig == null) {
        genesisConfig = GenesisConfig.mainnet();
      }
      if (genesisState == null) {
        genesisState = GenesisState.fromConfig(genesisConfig, protocolSchedule, new CodeCache());
      }
      blockchain = createInMemoryBlockchain(genesisState.getBlock());
    }
    if (networkId == null) {
      networkId = DEFAULT_NETWORK_ID;
    }
    if (worldStateArchive == null) {
      worldStateArchive =
          BlockchainSetupUtil.forTesting(DataStorageFormat.FOREST).getWorldArchive();
    }
    if (transactionPool == null) {
      transactionPool = mock(TransactionPool.class);
    }
    if (ethereumWireProtocolConfiguration == null) {
      ethereumWireProtocolConfiguration = EthProtocolConfiguration.DEFAULT;
    }
    if (forkIdManager == null) {
      forkIdManager =
          new ForkIdManager(blockchain, Collections.emptyList(), Collections.emptyList());
    }
    if (ethPeers == null) {
      ethPeers =
          new EthPeers(
              () -> protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()),
              TestClock.fixed(),
              new NoOpMetricsSystem(),
              EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
              Collections.emptyList(),
              Bytes.random(64),
              25,
              25,
              false,
              SyncMode.SNAP,
              forkIdManager);
    }
    ethPeers.setChainHeadTracker(EthProtocolManagerTestUtil.getChainHeadTrackerMock());
    if (ethMessages == null) {
      ethMessages = new EthMessages();
    }
    if (snapMessages == null) {
      snapMessages = new EthMessages();
    }
    if (ethScheduler == null) {
      ethScheduler =
          new DeterministicEthScheduler(DeterministicEthScheduler.TimeoutPolicy.NEVER_TIMEOUT);
    }
    if (peerTaskExecutor == null) {
      peerTaskExecutor = mock(PeerTaskExecutor.class);
    }
    if (ethContext == null) {
      ethContext =
          new EthContext(ethPeers, ethMessages, snapMessages, ethScheduler, peerTaskExecutor);
    }
    if (peerValidators == null) {
      peerValidators = Collections.emptyList();
    }
    if (mergePeerFilter.isEmpty()) {
      mergePeerFilter = Optional.of(new MergePeerFilter());
    }
    if (synchronizerConfiguration == null) {
      synchronizerConfiguration = SynchronizerConfiguration.builder().build();
    }
    return new EthProtocolManager(
        blockchain,
        networkId,
        worldStateArchive,
        transactionPool,
        ethereumWireProtocolConfiguration,
        ethPeers,
        ethMessages,
        ethContext,
        peerValidators,
        mergePeerFilter,
        synchronizerConfiguration,
        ethScheduler,
        forkIdManager);
  }
}
