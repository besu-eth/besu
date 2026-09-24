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
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Code written through the world state comes back with its jump destination analysis. */
class StoredJumpDestAnalysisTest {

  private static final Address CONTRACT =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  // PUSH1 0x5b; JUMPDEST; PUSH2 0x5b5b; JUMPDEST
  private static final Bytes CODE = Bytes.fromHexString("0x605b5b615b5b5b");

  private BonsaiWorldStateKeyValueStorage storage;
  private BonsaiWorldState worldState;

  @BeforeEach
  void setUp() {
    storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    worldState =
        new BonsaiWorldState(
            InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
                mock(Blockchain.class)),
            storage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());

    final WorldUpdater setup = worldState.updater();
    final MutableAccount contract = setup.createAccount(CONTRACT, 1, Wei.of(1L));
    contract.setCode(CODE);
    setup.commit();
    worldState.persist(null);
  }

  @Test
  void persistedCodeIsStoredWithItsAnalysis() {
    final Code stored = storage.getStoredCode(Hash.hash(CODE), CONTRACT.addressHash()).get();

    assertThat(stored.getBytes()).isEqualTo(CODE);
    assertThat(stored.getJumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(CODE));
  }

  @Test
  void loadedAccountUsesTheStoredAnalysis() {
    // a fresh cache, so the code has to come from storage
    final BonsaiWorldState reloaded =
        new BonsaiWorldState(
            InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
                mock(Blockchain.class)),
            storage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());

    final BonsaiAccount account = (BonsaiAccount) reloaded.updater().get(CONTRACT);
    final Code code = account.getOrCreateCachedCode();

    assertThat(code.getJumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(CODE));
    assertThat(code.isJumpDestInvalid(2)).isFalse();
    assertThat(code.isJumpDestInvalid(4)).isTrue();
  }

  @Test
  void codeWrittenDirectlyIsStoredWithItsAnalysis() {
    // the path snap sync takes
    final Bytes code = Bytes.fromHexString("0x5b5b60ff5b");
    storage.updater().putCode(Hash.EMPTY, Hash.hash(code), code).commit();

    final Code stored = storage.getStoredCode(Hash.hash(code), Hash.EMPTY).get();

    assertThat(stored.getBytes()).isEqualTo(code);
    assertThat(stored.getJumpDestBitMask()).containsExactly(0b10011L);
  }
}
