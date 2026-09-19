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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Accounts a block only reads must not be written back on persist. */
class ReadOnlyAccountPersistTest {

  private static final Address EOA =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address CONTRACT =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address MISSING =
      Address.fromHexString("0x3333333333333333333333333333333333333333");

  private InMemoryKeyValueStorageProvider provider;
  private BonsaiWorldState worldState;
  private Hash rootAfterSetup;

  @BeforeEach
  void setUp() {
    provider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            mock(Blockchain.class));
    worldState =
        new BonsaiWorldState(
            archive,
            new BonsaiWorldStateKeyValueStorage(
                provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());

    final WorldUpdater setup = worldState.updater();
    setup.createAccount(EOA, 1, Wei.of(1L));
    final MutableAccount contract = setup.createAccount(CONTRACT, 1, Wei.of(1L));
    contract.setCode(Bytes.of(0x5b, 0x5b));
    contract.setStorageValue(UInt256.ONE, UInt256.valueOf(7));
    setup.commit();
    worldState.persist(null);
    rootAfterSetup = worldState.rootHash();
  }

  @Test
  void readOnlyBlockWritesNothing() {
    final WorldUpdater updater = worldState.updater();
    updater.get(EOA).getBalance();
    updater.get(CONTRACT).getCode();
    updater.get(CONTRACT).getStorageValue(UInt256.ONE);
    assertThat(updater.get(MISSING)).isNull();
    updater.commit();

    final StateRootComputation computation =
        new DefaultStateRootCommitter().compute(worldState, null, worldState.updater());
    assertThat(computation.root()).isEqualTo(rootAfterSetup);

    final BonsaiWorldStateKeyValueStorage.Updater storageUpdater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    computation.applyTo(storageUpdater);
    verifyNoInteractions(storageUpdater);
  }

  @Test
  void readOnlyBlockLeavesNoAccountInTrieLog() {
    final WorldUpdater updater = worldState.updater();
    updater.get(EOA).getBalance();
    updater.get(CONTRACT).getStorageValue(UInt256.ONE);
    updater.commit();

    final BlockHeader header =
        new BlockHeaderTestFixture().number(1).stateRoot(rootAfterSetup).buildHeader();
    worldState.persist(header);

    assertThat(trieLogFor(header).getAccountChanges()).isEmpty();
  }

  @Test
  void changedAccountIsStillWritten() {
    final WorldUpdater updater = worldState.updater();
    updater.get(CONTRACT).getCode();
    updater.getAccount(EOA).incrementBalance(Wei.ONE);
    updater.commit();

    final StateRootComputation computation =
        new DefaultStateRootCommitter().compute(worldState, null, worldState.updater());
    assertThat(computation.root()).isNotEqualTo(rootAfterSetup);

    final BonsaiWorldStateKeyValueStorage.Updater storageUpdater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    computation.applyTo(storageUpdater);
    verify(storageUpdater).putAccountInfoState(eq(EOA.addressHash()), any());
    verify(storageUpdater, never()).putAccountInfoState(eq(CONTRACT.addressHash()), any());
  }

  @Test
  void storageOnlyChangeStillWritesTheAccount() {
    final WorldUpdater updater = worldState.updater();
    updater.getAccount(CONTRACT).setStorageValue(UInt256.ONE, UInt256.valueOf(8));
    updater.commit();

    final StateRootComputation computation =
        new DefaultStateRootCommitter().compute(worldState, null, worldState.updater());
    assertThat(computation.root()).isNotEqualTo(rootAfterSetup);

    final BonsaiWorldStateKeyValueStorage.Updater storageUpdater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    computation.applyTo(storageUpdater);
    verify(storageUpdater).putAccountInfoState(any(), any());
    verify(storageUpdater).putStorageValueBySlotHash(any(), any(), any());
  }

  private TrieLogLayer trieLogFor(final BlockHeader header) {
    final KeyValueStorage trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);
    final byte[] log =
        trieLogStorage.get(header.getHash().getBytes().toArrayUnsafe()).orElseThrow();
    return BonsaiTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(log), false));
  }
}
