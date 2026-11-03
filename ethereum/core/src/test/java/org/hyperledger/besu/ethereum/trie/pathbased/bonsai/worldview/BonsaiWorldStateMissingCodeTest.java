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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiWorldStateMissingCodeTest {

  private static final String GENESIS_CONFIG =
      "/org/hyperledger/besu/ethereum/mainnet/parallelization/genesis-it.json";
  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000000fffff");

  private static final DataStorageConfiguration CODE_BY_ACCOUNT_HASH =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.BONSAI)
          .extraStorageConfiguration(
              ImmutableExtraStorageConfiguration.builder()
                  .unstable(
                      ImmutableExtraStorageConfiguration.Unstable.builder()
                          .codeStoredByCodeHashEnabled(false)
                          .build())
                  .build())
          .build();

  private ExecutionContextTestFixture ctx;
  private BonsaiWorldStateProvider archive;
  private BonsaiWorldState worldState;
  private Hash codeHash;

  @BeforeEach
  void setUp() {
    useStorage(true);
  }

  @Test
  void missingCodeOfAStoredAccountThrows() {
    removeCode(archive.getWorldStateKeyValueStorage());

    assertThatThrownBy(() -> worldState.getCode(CONTRACT, codeHash))
        .isInstanceOfSatisfying(
            MissingCodeException.class,
            e -> {
              assertThat(e.getAddress()).isEqualTo(CONTRACT);
              assertThat(e.getCodeHash()).isEqualTo(codeHash);
            });
  }

  @Test
  void codeOfAnotherHashThanTheStoredOneIsEmpty() {
    // e.g. the prior lookup of a code set by a block access list
    assertThat(worldState.getCode(CONTRACT, Hash.hash(Bytes.of(1)))).isEmpty();
  }

  @Test
  void closedWorldStateReadsNoCode() {
    final BonsaiWorldState snapshot =
        (BonsaiWorldState)
            archive
                .getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(
                        ctx.getBlockchain().getChainHeadHeader()))
                .orElseThrow();
    snapshot.close();

    assertThat(snapshot.getCode(CONTRACT, codeHash)).isEmpty();
  }

  @Test
  void healRejectsACodeOfAnotherHash() {
    removeCode(archive.getWorldStateKeyValueStorage());
    archive.setCodeFetcher(hash -> Optional.of(Bytes.of(1)));

    assertThat(archive.healMissingCode(CONTRACT, codeHash)).isFalse();
    assertThatThrownBy(() -> worldState.getCode(CONTRACT, codeHash))
        .isInstanceOf(MissingCodeException.class);
  }

  @Test
  void healStoresAnOlderCodeHashByItsHash() {
    // e.g. an older code hash of the account, met while processing a block on an older state
    final Bytes otherCode = Bytes.of(1);
    archive.setCodeFetcher(hash -> Optional.of(otherCode));

    assertThat(archive.healMissingCode(CONTRACT, Hash.hash(otherCode))).isTrue();
    final BonsaiWorldStateKeyValueStorage storage = archive.getWorldStateKeyValueStorage();
    assertThat(storage.getCodeBytes(Hash.hash(otherCode), CONTRACT.addressHash()))
        .contains(otherCode);
    assertThat(storage.getCodeBytes(codeHash, CONTRACT.addressHash())).isPresent();
  }

  @Test
  void healRefusesAnotherCodeHashThanTheHeadOneByAccount() {
    useStorage(false);
    final Bytes otherCode = Bytes.of(1);
    final AtomicInteger fetches = new AtomicInteger();
    archive.setCodeFetcher(
        hash -> {
          fetches.incrementAndGet();
          return Optional.of(otherCode);
        });

    assertThat(archive.healMissingCode(CONTRACT, Hash.hash(otherCode))).isFalse();
    assertThat(fetches).as("refused before asking the peers").hasValue(0);
    // the account's code is kept
    assertThat(storedCode()).isNotEqualTo(otherCode);
  }

  @Test
  void healRefusesWhenTheHeadMovesDuringTheFetchByAccount() {
    useStorage(false);
    final Bytes code = storedCode();
    final Bytes newCode = Bytes.of(2);
    archive.setCodeFetcher(
        hash -> {
          // a block changes the account's code while the peers answer
          final WorldUpdater updater = worldState.updater();
          updater.getAccount(CONTRACT).setCode(newCode);
          updater.commit();
          worldState.persist(null);
          return Optional.of(code);
        });

    assertThat(archive.healMissingCode(CONTRACT, codeHash)).isFalse();
    assertThat(
            archive
                .getWorldStateKeyValueStorage()
                .getCodeBytes(Hash.hash(newCode), CONTRACT.addressHash()))
        .contains(newCode);
  }

  @Test
  void healFailsWhenTheFetcherThrows() {
    removeCode(archive.getWorldStateKeyValueStorage());
    archive.setCodeFetcher(
        hash -> {
          throw new IllegalStateException("peer request failed");
        });

    assertThat(archive.healMissingCode(CONTRACT, codeHash)).isFalse();
  }

  @Test
  void healFailsWhenTheStorageFails() {
    // with code stored by account the head account is read before the fetch and under the lock
    useStorage(false);
    final Bytes code = storedCode();
    removeCode(archive.getWorldStateKeyValueStorage());
    final BonsaiWorldStateProvider failingArchive = spy(archive);
    doCallRealMethod()
        .doThrow(new StorageException("disk failure"))
        .when(failingArchive)
        .getWorldState();
    failingArchive.setCodeFetcher(hash -> Optional.of(code));

    assertThat(failingArchive.healMissingCode(CONTRACT, codeHash)).isFalse();
  }

  private void useStorage(final boolean codeByCodeHash) {
    ctx =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .dataStorageConfiguration(
                codeByCodeHash
                    ? DataStorageConfiguration.DEFAULT_BONSAI_CONFIG
                    : CODE_BY_ACCOUNT_HASH)
            .build();
    archive = (BonsaiWorldStateProvider) ctx.getStateArchive();
    worldState = (BonsaiWorldState) archive.getWorldState();
    codeHash = worldState.get(CONTRACT).getCodeHash();
  }

  private Bytes storedCode() {
    return archive
        .getWorldStateKeyValueStorage()
        .getCodeBytes(codeHash, CONTRACT.addressHash())
        .orElseThrow();
  }

  private void removeCode(final BonsaiWorldStateKeyValueStorage storage) {
    // the code is keyed by code hash or by account hash, depending on the storage
    final SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();
    tx.remove(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe());
    tx.remove(CODE_STORAGE, CONTRACT.addressHash().getBytes().toArrayUnsafe());
    tx.commit();
  }
}
