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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.trie.common.MptStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionCollisionDetectorTest {

  private TransactionCollisionDetector collisionDetector;
  @Mock BonsaiWorldState worldState;
  BonsaiWorldStateUpdateAccumulator bonsaiUpdater;
  BonsaiWorldStateUpdateAccumulator trxUpdater;

  @BeforeEach
  public void setUp() {
    collisionDetector = new TransactionCollisionDetector();
    bonsaiUpdater =
        new BonsaiWorldStateUpdateAccumulator(
            worldState,
            (__, ___) -> {},
            (__, ___) -> {},
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache());
    trxUpdater =
        new BonsaiWorldStateUpdateAccumulator(
            worldState,
            (__, ___) -> {},
            (__, ___) -> {},
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache());
  }

  private Transaction createTransaction(final Address sender, final Address to) {
    return new Transaction.Builder()
        .nonce(1)
        .gasPrice(Wei.of(1))
        .gasLimit(21000)
        .to(to)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .chainId(BigInteger.ONE)
        .sender(sender)
        .build();
  }

  private BonsaiAccount createAccount(final Address address) {
    return new BonsaiAccount(
        worldState,
        address,
        Hash.hash(Address.ZERO.getBytes()),
        0,
        Wei.ONE,
        new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
        Hash.EMPTY,
        false,
        new BonsaiCodeCache());
  }

  @Test
  void testCollisionWithModifiedBalance() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setBalance(Wei.MAX_WEI);

    // Simulate that the address was already modified in the block
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected a collision with the modified address");
  }

  @Test
  void testCollisionWithModifiedNonce() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setNonce(1);

    // Simulate that the address was already modified in the block
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected a collision with the modified address");
  }

  @Test
  void testCollisionWithModifiedCode() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setCode(Bytes.repeat((byte) 0x01, 10));

    // Simulate that the address was already modified in the block
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected a collision with the modified address");
  }

  @Test
  void testCollisionWithModifiedStorageRootAndSameSlot() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setStorageRoot(Hash.EMPTY);
    final StorageSlotKey updateStorageSlotKey = new StorageSlotKey(UInt256.ONE);
    // Simulate that the address slot was already modified in the block
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));
    bonsaiUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(updateStorageSlotKey, new BonsaiValue<>(UInt256.ONE, UInt256.ZERO));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));
    trxUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(updateStorageSlotKey, new BonsaiValue<>(UInt256.ONE, UInt256.ONE));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected a collision with the modified address");
  }

  @Test
  void testCollisionWithModifiedStorageRootNotSameSlot() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setStorageRoot(Hash.EMPTY);
    // Simulate that the address slot was already modified in the block
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));
    bonsaiUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(new StorageSlotKey(UInt256.ZERO), new BonsaiValue<>(UInt256.ONE, UInt256.ZERO));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));
    trxUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(new StorageSlotKey(UInt256.ONE), new BonsaiValue<>(UInt256.ONE, UInt256.ONE));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertFalse(
        hasCollision,
        "Expected no collision when storage roots are modified but different slots are updated.");
  }

  @Test
  void testNoCollisionWhenTransactionTouchesAddressButNoStorage() {
    // Plain transfer to a contract whose storage was heavily written earlier in the block:
    // account details match excluding storage, tx storage is empty → no collision.
    final Address address = Address.fromHexString("0x1");
    final Address sender = Address.fromHexString("0x2");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setStorageRoot(Hash.EMPTY);

    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));
    final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> blockStorage =
        bonsaiUpdater
            .getStorageToUpdate()
            .computeIfAbsent(
                address,
                __ ->
                    new StorageConsumingMap<>(
                        address, new ConcurrentHashMap<>(), (___, ____) -> {}));
    for (int i = 0; i < 100; i++) {
      blockStorage.put(
          new StorageSlotKey(UInt256.valueOf(i)),
          new BonsaiValue<>(UInt256.ONE, UInt256.valueOf(i + 1)));
    }

    final Transaction transaction = createTransaction(sender, address);

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertFalse(hasCollision, "Expected no collision for a transfer that touches no storage slots");
  }

  @Test
  void testCollisionFindsOverlappingSlotAmongManyBlockSlots() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setStorageRoot(Hash.EMPTY);
    final StorageSlotKey overlappingSlot = new StorageSlotKey(UInt256.valueOf(50));

    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));
    final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> blockStorage =
        bonsaiUpdater
            .getStorageToUpdate()
            .computeIfAbsent(
                address,
                __ ->
                    new StorageConsumingMap<>(
                        address, new ConcurrentHashMap<>(), (___, ____) -> {}));
    for (int i = 0; i < 100; i++) {
      blockStorage.put(
          new StorageSlotKey(UInt256.valueOf(i)),
          new BonsaiValue<>(UInt256.ONE, UInt256.valueOf(i + 1)));
    }

    final Transaction transaction = createTransaction(address, address);
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));
    trxUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(overlappingSlot, new BonsaiValue<>(UInt256.ONE, UInt256.ONE));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected collision when one of many block slots overlaps");
  }

  @Test
  void testNoCollisionWhenBlockSlotIsUnchanged() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final StorageSlotKey slot = new StorageSlotKey(UInt256.ONE);

    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));
    bonsaiUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(slot, new BonsaiValue<>(UInt256.ONE, UInt256.ONE));

    final Transaction transaction = createTransaction(address, address);
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));
    trxUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(slot, new BonsaiValue<>(UInt256.ONE, UInt256.ONE));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertFalse(
        hasCollision, "Expected no collision when the block only read the overlapping slot");
  }

  @Test
  void testCollisionWhenTransactionReadsSlotModifiedByBlock() {
    // Stale read: tx only read a slot (unchanged in its accumulator) but the block
    // already modified that slot — must still detect a collision.
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final StorageSlotKey slot = new StorageSlotKey(UInt256.ONE);

    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));
    bonsaiUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(slot, new BonsaiValue<>(UInt256.ONE, UInt256.ZERO));

    final Transaction transaction = createTransaction(address, address);
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));
    trxUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(slot, new BonsaiValue<>(UInt256.ONE, UInt256.ONE));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(
        hasCollision, "Expected collision when tx read a slot that the block already modified");
  }

  @Test
  void testCollisionWithMiningBeneficiaryAddress() {
    final Address miningBeneficiary = Address.ZERO;
    final Address address = Address.fromHexString("0x1");

    final Transaction transaction = createTransaction(miningBeneficiary, address);

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            miningBeneficiary,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected collision with the mining beneficiary address as sender");
  }

  @Test
  void testCollisionWithAnotherMiningBeneficiaryAddress() {
    final Address miningBeneficiary = Address.ZERO;
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount miningBeneficiaryValue = createAccount(address);

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the mining beneficiary is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(miningBeneficiary, new BonsaiValue<>(miningBeneficiaryValue, miningBeneficiaryValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            miningBeneficiary,
            new ParallelizedTransactionContext(trxUpdater, null, true, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected collision with the read mining beneficiary address");
  }

  @Test
  void testCollisionWithDeletedAddress() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount accountValue = createAccount(address);

    // Simulate that the address was deleted in the block
    bonsaiUpdater.getAccountsToUpdate().put(address, new BonsaiValue<>(accountValue, null));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the deleted address is read in the next transaction
    trxUpdater.getAccountsToUpdate().put(address, new BonsaiValue<>(accountValue, accountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected a collision with the deleted address");
  }

  @Test
  void testCollisionWithNoModifiedAddress() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);

    // Simulate that the address was already read in the block
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater);

    assertFalse(hasCollision, "Expected no collision with the read address");
  }

  private BonsaiAccount createAccount(final Address address, final long nonce, final long balance) {
    return new BonsaiAccount(
        worldState,
        address,
        Hash.hash(address.getBytes()),
        nonce,
        Wei.of(balance),
        new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
        Hash.EMPTY,
        false,
        new BonsaiCodeCache());
  }

  private boolean hasCollisionCarryingCredits(
      final Transaction transaction, final List<Address> creditedAccounts) {
    return collisionDetector.hasCollision(
        transaction,
        Address.ZERO,
        new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
        bonsaiUpdater,
        creditedAccounts);
  }

  private Transaction creditScenario(final long creditedBalance) {
    final Address sender = Address.fromHexString("0x1");
    final Address credited = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    trxUpdater.recordBalanceObservations();
    trxUpdater.getAccountsToUpdate().put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    // an earlier transaction of the block paid the account
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(
            credited,
            new BonsaiValue<>(createAccount(credited, 1, 50), createAccount(credited, 1, 70)));
    trxUpdater
        .getAccountsToUpdate()
        .put(
            credited,
            new BonsaiValue<>(
                createAccount(credited, 1, 50), createAccount(credited, 1, creditedBalance)));
    return createTransaction(sender, credited);
  }

  @Test
  void creditToAnAccountWhoseBalanceChangedIsCarriedOver() {
    final List<Address> credited = new ArrayList<>();

    assertFalse(hasCollisionCarryingCredits(creditScenario(55), credited));
    assertThat(credited).containsExactly(Address.fromHexString("0x2"));
  }

  @Test
  void creditIsACollisionWhenCreditsAreNotCarriedOver() {
    assertTrue(hasStrictCollision(creditScenario(55)));
  }

  @Test
  void creditToAnAccountWhoseBalanceWasObservedIsACollision() {
    final Transaction transaction = creditScenario(55);
    trxUpdater.observeBalance(Address.fromHexString("0x2"));

    assertTrue(hasCollisionCarryingCredits(transaction, new ArrayList<>()));
  }

  @Test
  void spendingFromAnAccountWhoseBalanceChangedIsACollision() {
    assertTrue(hasCollisionCarryingCredits(creditScenario(45), new ArrayList<>()));
  }

  @Test
  void spendingWithinItsMarginFromAnAccountWhoseBalanceChangedIsCarriedOver() {
    final Transaction transaction = creditScenario(45);
    // the transaction found 50 where it spent 10
    trxUpdater.observeSufficientBalance(Address.fromHexString("0x2"), Wei.of(40));
    final List<Address> credited = new ArrayList<>();

    assertFalse(hasCollisionCarryingCredits(transaction, credited));
    assertThat(credited).containsExactly(Address.fromHexString("0x2"));
  }

  @Test
  void spendingBeyondItsMarginFromAnAccountWhoseBalanceChangedIsACollision() {
    final Address sender = Address.fromHexString("0x1");
    final Address spent = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    trxUpdater.recordBalanceObservations();
    trxUpdater.getAccountsToUpdate().put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    // an earlier transaction of the block took 30 of the 50
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(spent, new BonsaiValue<>(createAccount(spent, 1, 50), createAccount(spent, 1, 20)));
    trxUpdater
        .getAccountsToUpdate()
        .put(spent, new BonsaiValue<>(createAccount(spent, 1, 50), createAccount(spent, 1, 5)));
    // the transaction found 50 where it spent 45
    trxUpdater.observeSufficientBalance(spent, Wei.of(5));

    assertTrue(hasCollisionCarryingCredits(createTransaction(sender, spent), new ArrayList<>()));
  }

  @Test
  void creditWithoutRecordedObservationsIsACollision() {
    final Transaction transaction = creditScenario(55);
    final BonsaiWorldStateUpdateAccumulator unrecorded =
        new BonsaiWorldStateUpdateAccumulator(
            worldState,
            (__, ___) -> {},
            (__, ___) -> {},
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache());
    unrecorded.getAccountsToUpdate().putAll(trxUpdater.getAccountsToUpdate());

    assertTrue(
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(unrecorded, null, false, Wei.ZERO),
            bonsaiUpdater,
            new ArrayList<>()));
  }

  @Test
  void creditToAnAccountTheBlockEmptiedIsACollision() {
    final Address sender = Address.fromHexString("0x1");
    final Address credited = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    trxUpdater.recordBalanceObservations();
    trxUpdater.getAccountsToUpdate().put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(
            credited,
            new BonsaiValue<>(createAccount(credited, 0, 50), createAccount(credited, 0, 0)));
    trxUpdater
        .getAccountsToUpdate()
        .put(
            credited,
            new BonsaiValue<>(createAccount(credited, 0, 50), createAccount(credited, 0, 60)));

    assertTrue(hasCollisionCarryingCredits(createTransaction(sender, credited), new ArrayList<>()));
  }

  @Test
  void balanceChangeOfTheSenderIsACollision() {
    final Address sender = Address.fromHexString("0x1");
    trxUpdater.recordBalanceObservations();
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(createAccount(sender, 1, 50), createAccount(sender, 1, 70)));
    trxUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(createAccount(sender, 1, 50), createAccount(sender, 2, 40)));

    assertTrue(
        hasCollisionCarryingCredits(
            createTransaction(sender, Address.fromHexString("0x3")), new ArrayList<>()));
  }

  private boolean hasStrictCollision(final Transaction transaction) {
    return collisionDetector.hasCollision(
        transaction,
        Address.ZERO,
        new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
        bonsaiUpdater);
  }
}
