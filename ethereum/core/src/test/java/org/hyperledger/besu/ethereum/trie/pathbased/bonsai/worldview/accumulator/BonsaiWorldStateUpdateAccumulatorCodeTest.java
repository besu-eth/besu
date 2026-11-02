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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link BonsaiWorldStateUpdateAccumulator#getCode}. */
class BonsaiWorldStateUpdateAccumulatorCodeTest {

  private static final Address CONTRACT =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  // PUSH1 0x5b; JUMPDEST; PUSH2 0x5b5b; JUMPDEST
  private static final Bytes CODE = Bytes.fromHexString("0x605b5b615b5b5b");
  private static final Hash CODE_HASH = Hash.hash(CODE);

  private BonsaiWorldState worldState;
  private BonsaiWorldStateUpdateAccumulator accumulator;

  @BeforeEach
  void setUp() {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    worldState =
        new BonsaiWorldState(
            storage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                storage, EvmConfiguration.DEFAULT, new BonsaiCodeCache()),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());
    accumulator = (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    accumulator.createAccount(CONTRACT, 1L, Wei.ONE).setCode(CODE);
    accumulator.commit();
    worldState.persist(null);
    // so that the code has to come from the storage
    accumulator.reset();
  }

  @AfterEach
  void tearDown() {
    worldState.close();
  }

  @Test
  void storedCodeComesWithItsAnalysis() {
    final Code code = accumulator.getCode(CONTRACT, CODE_HASH).orElseThrow();

    assertThat(code.getBytes()).isEqualTo(CODE);
    assertThat(code.getJumpDestBitMask()).containsExactly(0b1000100L);
  }

  @Test
  void secondAccessKeepsTheAnalysis() {
    final Code first = accumulator.getCode(CONTRACT, CODE_HASH).orElseThrow();

    assertThat(accumulator.getCode(CONTRACT, CODE_HASH)).containsSame(first);
  }

  @Test
  void clonedAccumulatorKeepsTheAnalysis() {
    final Code loaded = accumulator.getCode(CONTRACT, CODE_HASH).orElseThrow();

    final BonsaiWorldStateUpdateAccumulator clone =
        (BonsaiWorldStateUpdateAccumulator) accumulator.copy();

    assertThat(clone.getCode(CONTRACT, CODE_HASH)).containsSame(loaded);
  }

  @Test
  void codeSetAfterTheLoadReplacesTheLoadedCode() {
    accumulator.getCode(CONTRACT, CODE_HASH);
    final Bytes newCode = Bytes.fromHexString("0x5b00");

    accumulator.getAccount(CONTRACT).setCode(newCode);
    accumulator.commit();

    final Code code = accumulator.getCode(CONTRACT, Hash.hash(newCode)).orElseThrow();
    assertThat(code.getBytes()).isEqualTo(newCode);
    assertThat(code.isJumpDestInvalid(0)).isFalse();
    assertThat(code.isJumpDestInvalid(1)).isTrue();
  }

  @Test
  void missingCodeIsEmptyOnEveryAccess() {
    final Address unknown = Address.fromHexString("0x3333333333333333333333333333333333333333");
    final Hash unknownHash = Hash.hash(Bytes.of(1));

    assertThat(accumulator.getCode(unknown, unknownHash)).isEmpty();
    assertThat(accumulator.getCode(unknown, unknownHash)).isEmpty();
  }
}
