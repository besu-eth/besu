/*
 * Copyright contributors to Hyperledger Besu.
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
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BonsaiFrontierRootHashTest {

  private static final Address CONTRACT =
      Address.fromHexString("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
  private static final Address ACCOUNT_B =
      Address.fromHexString("0x1000000000000000000000000000000000000002");
  private static final Address ACCOUNT_C =
      Address.fromHexString("0x1000000000000000000000000000000000000003");

  @Test
  void frontierRootHashMatchesFullRecalculationWhenSameAccountChangesTwice() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();

    final WorldUpdater firstUpdater = worldState.updater();
    final MutableAccount firstAccount = firstUpdater.getAccount(CONTRACT);
    firstAccount.setBalance(Wei.of(2));
    firstAccount.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    firstUpdater.commit();
    firstUpdater.markTransactionBoundary();

    // frontierRootHash reflects the state after TX 1 (unlike rootHash which stays at the
    // persisted block root until persist() is called)
    final Hash firstFrontierRoot = worldState.frontierRootHash();
    assertThat(firstFrontierRoot).isEqualTo(fullRecalculatedRoot(worldState));

    final WorldUpdater secondUpdater = worldState.updater();
    final MutableAccount secondAccount = secondUpdater.getAccount(CONTRACT);
    secondAccount.setBalance(Wei.of(3));
    secondAccount.setStorageValue(UInt256.ONE, UInt256.valueOf(3));
    secondUpdater.commit();
    secondUpdater.markTransactionBoundary();

    // frontierRootHash advances with each transaction within the block
    final Hash secondFrontierRoot = worldState.frontierRootHash();
    assertThat(secondFrontierRoot).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(secondFrontierRoot).isNotEqualTo(firstFrontierRoot);
  }

  @Test
  void frontierRootHashMatchesFullRecalculationWhenStorageIsClearedWithoutNewSlots() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();

    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.getAccount(CONTRACT);
    account.clearStorage();
    updater.commit();
    updater.markTransactionBoundary();

    final Hash afterClear = worldState.frontierRootHash();
    assertThat(afterClear).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterClear)
        .as("Clearing storage should change the root hash")
        .isNotEqualTo(worldState.rootHash());
  }

  @Test
  void frontierRootHashHandlesDeleteThenRecreateInSameBlock() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();

    final WorldUpdater deleter = worldState.updater();
    deleter.deleteAccount(CONTRACT);
    deleter.commit();
    deleter.markTransactionBoundary();

    final Hash afterDelete = worldState.frontierRootHash();
    assertThat(afterDelete).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterDelete)
        .as("Deleting an account should change the root hash")
        .isNotEqualTo(worldState.rootHash());

    final WorldUpdater creator = worldState.updater();
    final MutableAccount recreated = creator.createAccount(CONTRACT);
    recreated.setBalance(Wei.of(999));
    recreated.setStorageValue(UInt256.valueOf(7), UInt256.valueOf(7));
    creator.commit();
    creator.markTransactionBoundary();

    final Hash afterRecreate = worldState.frontierRootHash();
    assertThat(afterRecreate).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterRecreate).isNotEqualTo(afterDelete);
  }

  @Test
  void frontierRootHashResetsCorrectlyAcrossBlockBoundary() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();

    final WorldUpdater blockNUpdater = worldState.updater();
    blockNUpdater.getAccount(CONTRACT).setBalance(Wei.of(50));
    blockNUpdater.commit();
    blockNUpdater.markTransactionBoundary();
    final Hash blockNRoot = worldState.frontierRootHash();
    assertThat(blockNRoot).isEqualTo(fullRecalculatedRoot(worldState));

    worldState.persist(null);

    final WorldUpdater blockN1Updater = worldState.updater();
    blockN1Updater.getAccount(CONTRACT).setBalance(Wei.of(75));
    blockN1Updater.commit();
    blockN1Updater.markTransactionBoundary();
    final Hash blockN1Root = worldState.frontierRootHash();
    assertThat(blockN1Root).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(blockN1Root).isNotEqualTo(blockNRoot);
  }

  @Test
  void frontierRootHashRespectsTrieDisabledConfig() {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    final BonsaiWorldState worldState = (BonsaiWorldState) archive.getWorldState();

    final WorldUpdater setup = worldState.updater();
    setup.createAccount(CONTRACT).setBalance(Wei.of(1));
    setup.commit();
    setup.markTransactionBoundary();
    worldState.persist(null);

    worldState.disableTrie();

    final WorldUpdater updater = worldState.updater();
    updater.getAccount(CONTRACT).setBalance(Wei.of(42));
    updater.commit();
    updater.markTransactionBoundary();

    assertThat(worldState.frontierRootHash()).isNotNull();
  }

  @Test
  void frontierRootHashMatchesWhenDifferentAccountsDirtyInDifferentTransactions() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();

    // TX1 only touches CONTRACT
    final WorldUpdater tx1 = worldState.updater();
    tx1.getAccount(CONTRACT).setBalance(Wei.of(10));
    tx1.commit();
    tx1.markTransactionBoundary();

    final Hash afterTx1 = worldState.frontierRootHash();
    assertThat(afterTx1).isEqualTo(fullRecalculatedRoot(worldState));

    // TX2 only touches ACCOUNT_B (a new account, not persisted)
    final WorldUpdater tx2 = worldState.updater();
    tx2.createAccount(ACCOUNT_B).setBalance(Wei.of(77));
    tx2.commit();
    tx2.markTransactionBoundary();

    // The tracker should process only ACCOUNT_B for this call, but the result
    // must still reflect both TX1 and TX2
    final Hash afterTx2 = worldState.frontierRootHash();
    assertThat(afterTx2).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterTx2).isNotEqualTo(afterTx1);

    // TX3 touches a third account
    final WorldUpdater tx3 = worldState.updater();
    tx3.createAccount(ACCOUNT_C).setBalance(Wei.of(33));
    tx3.commit();
    tx3.markTransactionBoundary();

    final Hash afterTx3 = worldState.frontierRootHash();
    assertThat(afterTx3).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterTx3).isNotEqualTo(afterTx2);
  }

  @Test
  void frontierRootHashMatchesWhenNewAccountCreatedMidBlock() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();

    // Create a brand-new account (not in persisted state) with storage
    final WorldUpdater updater = worldState.updater();
    final MutableAccount newAccount = updater.createAccount(ACCOUNT_B);
    newAccount.setBalance(Wei.of(500));
    newAccount.setStorageValue(UInt256.valueOf(42), UInt256.valueOf(42));
    updater.commit();
    updater.markTransactionBoundary();

    final Hash afterCreate = worldState.frontierRootHash();
    assertThat(afterCreate).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterCreate)
        .as("Creating a new account should change the root hash")
        .isNotEqualTo(worldState.rootHash());
  }

  @Test
  void frontierRootHashReflectsChangesImportedViaImportStateChangesFromSource() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();
    final Hash baselineRoot = worldState.frontierRootHash();

    final BonsaiWorldStateUpdateAccumulator blockAccumulator =
        (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator();
    final BonsaiWorldStateUpdateAccumulator txAccumulator = blockAccumulator.copy();

    final MutableAccount account = txAccumulator.getAccount(CONTRACT);
    account.setBalance(Wei.of(123));
    account.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(2));
    txAccumulator.commit();

    blockAccumulator.importStateChangesFromSource(txAccumulator);
    worldState.getAccumulator().markTransactionBoundary();

    final Hash afterImport = worldState.frontierRootHash();
    assertThat(afterImport).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterImport).isNotEqualTo(baselineRoot);
  }

  @Test
  void revertKeepsFrontierDirtyAddressesForAlreadyCommittedChanges() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();
    final Hash baselineRoot = worldState.frontierRootHash();

    final WorldUpdater committed = worldState.updater();
    committed.getAccount(CONTRACT).setBalance(Wei.of(42));
    committed.commit();
    committed.markTransactionBoundary();
    final Hash afterCommit = worldState.frontierRootHash();
    assertThat(afterCommit).isNotEqualTo(baselineRoot);

    // Start uncommitted work, then revert. The already-committed change must remain visible.
    final WorldUpdater uncommitted = worldState.updater();
    uncommitted.getAccount(CONTRACT).setBalance(Wei.of(999));
    ((BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator()).revert();

    final Hash afterRevert = worldState.frontierRootHash();
    assertThat(afterRevert).isEqualTo(afterCommit);
    assertThat(afterRevert).isEqualTo(fullRecalculatedRoot(worldState));
  }

  @Test
  void resetClearsFrontierDirtyAddresses() {
    final BonsaiWorldState worldState = createWorldStateWithContractStorage();

    final WorldUpdater updater = worldState.updater();
    updater.getAccount(CONTRACT).setBalance(Wei.of(42));
    updater.commit();
    updater.markTransactionBoundary();

    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator();
    assertThat(accumulator.getFrontierDirtyAddresses()).contains(CONTRACT);

    accumulator.reset();
    assertThat(accumulator.getFrontierDirtyAddresses()).isEmpty();
  }

  private static BonsaiWorldState createWorldStateWithContractStorage() {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    final BonsaiWorldState worldState = (BonsaiWorldState) archive.getWorldState();

    final WorldUpdater setup = worldState.updater();
    final MutableAccount account = setup.createAccount(CONTRACT);
    account.setBalance(Wei.of(1));
    account.setStorageValue(UInt256.ONE, UInt256.ONE);
    setup.commit();
    setup.markTransactionBoundary();
    worldState.persist(null);

    return worldState;
  }

  /**
   * Canonical oracle: copies the entire accumulator and rebuilds the trie from scratch. This is the
   * O(N²) path that the incremental tracker replaces, used here as a reference to verify
   * correctness — it cannot produce a wrong result.
   */
  private static Hash fullRecalculatedRoot(final BonsaiWorldState worldState) {
    final BonsaiWorldStateUpdateAccumulator accumulatorCopy =
        (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator().copy();
    return worldState.calculateRootHash(Optional.empty(), accumulatorCopy);
  }
}
