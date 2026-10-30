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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.MissingCodeException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.Consumer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BonsaiWorldStateUpdateAccumulatorParallelCommitTest {

  private static final Bytes CODE = Bytes.fromHexString("0x600160005500");
  private static final Hash CODE_HASH = Hash.hash(CODE);
  private static final Bytes NEW_CODE = Bytes.fromHexString("0x5b00");
  // enough updated accounts for commit to process them in parallel
  private static final List<Address> ACCOUNTS =
      IntStream.rangeClosed(1, 96)
          .mapToObj(i -> Address.fromHexString(Integer.toHexString(i)))
          .toList();
  private static final Address FAILING = ACCOUNTS.get(0);
  private static final Consumer<BonsaiValue<BonsaiAccount>> NO_ACCOUNT_PRELOAD =
      (address, account) -> {};
  private static final Consumer<StorageSlotKey> NO_SLOT_PRELOAD = (address, slot) -> {};

  private BonsaiWorldState worldState;

  @AfterEach
  void tearDown() {
    worldState.close();
  }

  @Test
  void failedCommitTaskIsRethrownOnceTheOthersAreDone() {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final BonsaiCodeCache codeCache = new BonsaiCodeCache();
    worldState =
        new BonsaiWorldState(
            storage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(storage, EvmConfiguration.DEFAULT, codeCache),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            codeCache);
    final var setup = worldState.updater();
    ACCOUNTS.forEach(address -> setup.createAccount(address, 1L, Wei.ONE).setCode(CODE));
    setup.commit();
    worldState.persist(null);

    // the prior code read of one account fails at once, the others take a while
    final BonsaiWorldState world = spy(worldState);
    doAnswer(
            invocation -> {
              if (FAILING.equals(invocation.getArgument(0))) {
                throw new MissingCodeException(FAILING, CODE_HASH);
              }
              Thread.sleep(20);
              return invocation.callRealMethod();
            })
        .when(world)
        .getCode(any(), any());
    final BonsaiWorldStateUpdateAccumulator accumulator =
        new BonsaiWorldStateUpdateAccumulator(
            world, NO_ACCOUNT_PRELOAD, NO_SLOT_PRELOAD, EvmConfiguration.DEFAULT, codeCache);
    // the accounts are opened from the code cache, so their code is first read in commit
    codeCache.put(CODE_HASH, new Code(CODE));
    ACCOUNTS.forEach(address -> accumulator.getAccount(address).setCode(NEW_CODE));

    assertThatThrownBy(accumulator::commit).isInstanceOf(MissingCodeException.class);
    // a reset right now would clear everything, no task still writes
    assertThat(ACCOUNTS.stream().skip(1).map(accumulator.getCodeToUpdate()::get))
        .allSatisfy(code -> assertThat(code.getUpdated()).isEqualTo(NEW_CODE));
  }
}
