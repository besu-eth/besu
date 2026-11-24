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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.CodeDelegation;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.parallelization.TransactionCollisionDetector.CarriedAccount;
import org.hyperledger.besu.ethereum.trie.common.MptStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

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
  BonsaiWorldStateUpdateAccumulator predecessorUpdater;

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
    predecessorUpdater =
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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

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
            bonsaiUpdater,
            new HashMap<>());

    assertTrue(
        hasCollision, "Expected collision when tx read a slot that the block already modified");
  }

  @Test
  void testCollisionWhenTransactionReadsSlotOfStorageClearedByBlock() {
    // Before Cancun, SELFDESTRUCT and a CREATE2 of the same code re-create an account with the
    // same nonce, balance and code but empty storage, and the block keeps no entries of its slots.
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount recreatedAccountValue =
        new BonsaiAccount(priorAccountValue, worldState, true);
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, recreatedAccountValue));
    bonsaiUpdater.getStorageToClear().add(address);

    final Transaction transaction = createTransaction(address, address);
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
            bonsaiUpdater,
            new HashMap<>());

    assertTrue(hasCollision, "Expected collision when tx read a slot of storage the block cleared");
  }

  @Test
  void testNoCollisionWhenTransactionTouchesNoSlotOfStorageClearedByBlock() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount recreatedAccountValue =
        new BonsaiAccount(priorAccountValue, worldState, true);
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, recreatedAccountValue));
    bonsaiUpdater.getStorageToClear().add(address);

    final Transaction transaction = createTransaction(address, address);
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater,
            new HashMap<>());

    assertFalse(
        hasCollision,
        "Expected no collision when tx touched no slot of the storage the block cleared");
  }

  @Test
  void miningBeneficiaryAsSenderIsCarriedOverWhenTheBlockOnlyRaisedItsBalance() {
    final Address beneficiary = Address.ZERO;
    trxUpdater.recordBalanceObservations();
    // earlier transactions paid their fees to the beneficiary
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(
            beneficiary,
            new BonsaiValue<>(
                createAccount(beneficiary, 1, 50), createAccount(beneficiary, 1, 60)));
    trxUpdater
        .getAccountsToUpdate()
        .put(
            beneficiary,
            new BonsaiValue<>(
                createAccount(beneficiary, 1, 50), createAccount(beneficiary, 2, 40)));

    final Map<Address, CarriedAccount> carried = new HashMap<>();

    assertFalse(
        hasCollisionCarryingCredits(
            createTransaction(beneficiary, Address.fromHexString("0x2")), carried),
        "Expected no collision as the fees only raised the balance that paid for the gas");
    assertThat(carried).containsOnlyKeys(beneficiary);
  }

  @Test
  void miningBeneficiaryWhoseBalanceWasReadCollidesEvenCarryingCredits() {
    final Address sender = Address.fromHexString("0x1");
    final Address beneficiary = Address.ZERO;
    trxUpdater.recordBalanceObservations();
    trxUpdater.observeBalance(beneficiary);
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(
            beneficiary,
            new BonsaiValue<>(
                createAccount(beneficiary, 1, 50), createAccount(beneficiary, 1, 60)));
    trxUpdater
        .getAccountsToUpdate()
        .put(
            beneficiary,
            new BonsaiValue<>(
                createAccount(beneficiary, 1, 50), createAccount(beneficiary, 1, 50)));

    assertTrue(
        hasCollisionCarryingCredits(createTransaction(sender, beneficiary), new HashMap<>()),
        "Expected a collision as the transaction read the beneficiary's balance");
  }

  @Test
  void paymentToTheMiningBeneficiaryIsCarriedOver() {
    final Address sender = Address.fromHexString("0x1");
    final Address beneficiary = Address.ZERO;
    trxUpdater.recordBalanceObservations();
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(
            beneficiary,
            new BonsaiValue<>(
                createAccount(beneficiary, 1, 50), createAccount(beneficiary, 1, 60)));
    trxUpdater
        .getAccountsToUpdate()
        .put(
            beneficiary,
            new BonsaiValue<>(
                createAccount(beneficiary, 1, 50), createAccount(beneficiary, 1, 55)));
    final Map<Address, CarriedAccount> carriedAccounts = new HashMap<>();

    assertFalse(
        hasCollisionCarryingCredits(createTransaction(sender, beneficiary), carriedAccounts),
        "Expected the payment to the beneficiary to be carried over");
    assertThat(carriedAccounts).containsOnlyKeys(beneficiary);
  }

  @Test
  void testCollisionWithDeletedAddress() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount accountValue = createAccount(address);
    trxUpdater.recordBalanceObservations();

    // Simulate that the address was deleted in the block
    bonsaiUpdater.getAccountsToUpdate().put(address, new BonsaiValue<>(accountValue, null));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the deleted address is read in the next transaction
    trxUpdater.getAccountsToUpdate().put(address, new BonsaiValue<>(accountValue, accountValue));

    assertTrue(
        hasCollisionCarryingCredits(transaction, new HashMap<>()),
        "Expected a collision with the deleted address");
  }

  @Test
  void testCollisionWithAddressDeletedByTransaction() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount priorAccountValue = createAccount(address);
    final BonsaiAccount nextAccountValue = new BonsaiAccount(priorAccountValue, worldState, true);
    nextAccountValue.setBalance(Wei.MAX_WEI);
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(address, new BonsaiValue<>(priorAccountValue, nextAccountValue));

    final Transaction transaction =
        createTransaction(Address.fromHexString("0x2"), Address.fromHexString("0x3"));
    trxUpdater.recordBalanceObservations();
    // the detector relies on the commit to list the deleted account among the updated ones
    trxUpdater.deleteAccount(address);
    trxUpdater.commit();

    assertTrue(
        hasCollisionCarryingCredits(transaction, new HashMap<>()),
        "Expected a collision with the address the transaction deleted");
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
            bonsaiUpdater,
            new HashMap<>());

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

  private BonsaiAccount createContract(final Address address, final long nonce) {
    return new BonsaiAccount(
        worldState,
        address,
        Hash.hash(address.getBytes()),
        nonce,
        Wei.of(50),
        new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
        Hash.hash(Bytes.fromHexString("0x00")),
        false,
        new BonsaiCodeCache());
  }

  private static void putSlot(
      final BonsaiWorldStateUpdateAccumulator accumulator,
      final Address address,
      final long slot,
      final UInt256 prior,
      final UInt256 updated) {
    accumulator
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(new StorageSlotKey(UInt256.valueOf(slot)), new BonsaiValue<>(prior, updated));
  }

  private boolean hasChainedCollision(final Transaction transaction) {
    return collisionDetector.hasCollision(
        transaction,
        Address.ZERO,
        new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO, predecessorUpdater),
        bonsaiUpdater,
        new HashMap<>());
  }

  @Test
  void chainedTransactionHasNoCollisionWhenBlockHoldsPredecessorSenderState() {
    final Address sender = Address.fromHexString("0x1");
    final Address recipient = Address.fromHexString("0x2");
    final BonsaiAccount parent = createAccount(sender, 0, 100);
    final BonsaiAccount afterPredecessor = createAccount(sender, 1, 90);

    predecessorUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, afterPredecessor));
    bonsaiUpdater.getAccountsToUpdate().put(sender, new BonsaiValue<>(parent, afterPredecessor));
    trxUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, createAccount(sender, 2, 80)));

    assertFalse(
        hasChainedCollision(createTransaction(sender, recipient)),
        "Expected no collision when the block holds the state the chain assumed");
  }

  @Test
  void runThatDeletedTheMiningBeneficiaryCollidesEvenCarryingCredits() {
    final Address sender = Address.fromHexString("0x1");
    final Address beneficiary = Address.fromHexString("0x9");
    trxUpdater.recordBalanceObservations();
    trxUpdater
        .getAccountsToUpdate()
        .put(
            sender, new BonsaiValue<>(createAccount(sender, 0, 100), createAccount(sender, 1, 90)));
    trxUpdater.deleteAccount(beneficiary);
    trxUpdater.commit();
    // the worker drops a beneficiary the transaction touched only for its fee
    trxUpdater.getAccountsToUpdate().remove(beneficiary);
    trxUpdater.getStorageToClear().remove(beneficiary);
    // the block still holds the beneficiary as an existing empty account
    final BonsaiAccount emptyBeneficiary = createAccount(beneficiary, 0, 0);
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(beneficiary, new BonsaiValue<>(emptyBeneficiary, emptyBeneficiary));

    assertTrue(
        collisionDetector.hasCollision(
            createTransaction(sender, Address.fromHexString("0x2")),
            beneficiary,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater,
            new HashMap<>()),
        "Expected a collision as taking the result would not delete the beneficiary");
  }

  @Test
  void runThatDeletedAMiningBeneficiaryTheBlockDoesNotHoldHasNoCollision() {
    final Address sender = Address.fromHexString("0x1");
    final Address beneficiary = Address.fromHexString("0x9");
    trxUpdater.recordBalanceObservations();
    trxUpdater
        .getAccountsToUpdate()
        .put(
            sender, new BonsaiValue<>(createAccount(sender, 0, 100), createAccount(sender, 1, 90)));
    // the fee credit created the beneficiary, and state clearing deleted it again
    trxUpdater.deleteAccount(beneficiary);
    trxUpdater.commit();
    trxUpdater.getAccountsToUpdate().remove(beneficiary);
    trxUpdater.getStorageToClear().remove(beneficiary);

    assertFalse(
        collisionDetector.hasCollision(
            createTransaction(sender, Address.fromHexString("0x2")),
            beneficiary,
            new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
            bonsaiUpdater,
            new HashMap<>()),
        "Expected no collision as the block does not hold the beneficiary either");
  }

  @Test
  void chainedTransactionThatTouchedTheMiningBeneficiaryCollidesEvenCarryingCredits() {
    final Address sender = Address.fromHexString("0x1");
    final Address beneficiary = Address.fromHexString("0x9");
    final BonsaiAccount parent = createAccount(sender, 0, 100);
    final BonsaiAccount afterPredecessor = createAccount(sender, 1, 90);

    predecessorUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, afterPredecessor));
    bonsaiUpdater.getAccountsToUpdate().put(sender, new BonsaiValue<>(parent, afterPredecessor));
    trxUpdater.recordBalanceObservations();
    trxUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, createAccount(sender, 2, 80)));
    trxUpdater
        .getAccountsToUpdate()
        .put(
            beneficiary,
            new BonsaiValue<>(
                createAccount(beneficiary, 1, 50), createAccount(beneficiary, 1, 60)));

    assertTrue(
        collisionDetector.hasCollision(
            createTransaction(sender, beneficiary),
            beneficiary,
            new ParallelizedTransactionContext(
                trxUpdater, null, true, Wei.ZERO, predecessorUpdater),
            bonsaiUpdater,
            new HashMap<>()),
        "Expected a collision as a chained result carries no balance over");
  }

  @Test
  void chainedTransactionCollidesWhenBlockSenderDiffersFromPredecessor() {
    final Address sender = Address.fromHexString("0x1");
    final Address recipient = Address.fromHexString("0x2");
    final BonsaiAccount parent = createAccount(sender, 0, 100);

    predecessorUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, createAccount(sender, 1, 90)));
    // the predecessor was executed again and used a different amount of gas
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, createAccount(sender, 1, 85)));
    trxUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, createAccount(sender, 2, 80)));

    assertTrue(
        hasChainedCollision(createTransaction(sender, recipient)),
        "Expected a collision when the block's sender differs from what the chain assumed");
  }

  @Test
  void chainedTransactionCollidesWhenPredecessorChangeIsNotInBlock() {
    final Address sender = Address.fromHexString("0x1");
    final Address recipient = Address.fromHexString("0x2");
    final BonsaiAccount parent = createAccount(sender, 0, 100);

    predecessorUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, createAccount(sender, 1, 90)));
    trxUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(parent, createAccount(sender, 2, 80)));

    assertTrue(
        hasChainedCollision(createTransaction(sender, recipient)),
        "Expected a collision when the block lacks the predecessor's change");
  }

  @Test
  void chainedTransactionCollidesWhenSlotOfPredecessorWasWrittenAgain() {
    final Address sender = Address.fromHexString("0x1");
    final Address contract = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    final BonsaiAccount contractAccount = createAccount(contract, 1, 0);
    for (final BonsaiWorldStateUpdateAccumulator accumulator :
        new BonsaiWorldStateUpdateAccumulator[] {bonsaiUpdater, predecessorUpdater, trxUpdater}) {
      accumulator
          .getAccountsToUpdate()
          .put(contract, new BonsaiValue<>(contractAccount, contractAccount));
    }
    predecessorUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    trxUpdater.getAccountsToUpdate().put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    putSlot(predecessorUpdater, contract, 1, UInt256.ONE, UInt256.valueOf(2));
    // a transaction between the two wrote the slot again
    putSlot(bonsaiUpdater, contract, 1, UInt256.ONE, UInt256.valueOf(3));
    putSlot(trxUpdater, contract, 1, UInt256.ONE, UInt256.valueOf(2));

    assertTrue(
        hasChainedCollision(createTransaction(sender, contract)),
        "Expected a collision when a slot the chain wrote was written again");
  }

  @Test
  void chainedTransactionHasNoCollisionWhenSlotHoldsPredecessorValue() {
    final Address sender = Address.fromHexString("0x1");
    final Address contract = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    final BonsaiAccount contractAccount = createAccount(contract, 1, 0);
    for (final BonsaiWorldStateUpdateAccumulator accumulator :
        new BonsaiWorldStateUpdateAccumulator[] {bonsaiUpdater, predecessorUpdater, trxUpdater}) {
      accumulator
          .getAccountsToUpdate()
          .put(contract, new BonsaiValue<>(contractAccount, contractAccount));
      accumulator
          .getAccountsToUpdate()
          .put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    }
    putSlot(predecessorUpdater, contract, 1, UInt256.ONE, UInt256.valueOf(2));
    putSlot(bonsaiUpdater, contract, 1, UInt256.ONE, UInt256.valueOf(2));
    putSlot(trxUpdater, contract, 1, UInt256.ONE, UInt256.valueOf(5));

    assertFalse(
        hasChainedCollision(createTransaction(sender, contract)),
        "Expected no collision when the slot holds the predecessor's value");
  }

  @Test
  void chainedTransactionCollidesWhenSlotItReadFromParentChanged() {
    final Address sender = Address.fromHexString("0x1");
    final Address contract = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    final BonsaiAccount contractAccount = createAccount(contract, 1, 0);
    for (final BonsaiWorldStateUpdateAccumulator accumulator :
        new BonsaiWorldStateUpdateAccumulator[] {bonsaiUpdater, predecessorUpdater, trxUpdater}) {
      accumulator
          .getAccountsToUpdate()
          .put(contract, new BonsaiValue<>(contractAccount, contractAccount));
      accumulator
          .getAccountsToUpdate()
          .put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    }
    putSlot(bonsaiUpdater, contract, 7, UInt256.valueOf(4), UInt256.valueOf(6));
    putSlot(trxUpdater, contract, 7, UInt256.valueOf(4), UInt256.valueOf(4));

    assertTrue(
        hasChainedCollision(createTransaction(sender, contract)),
        "Expected a collision when a slot read from the parent state changed");
  }

  @Test
  void chainedTransactionReadsAbsentAndZeroSlotsAsEqual() {
    final Address sender = Address.fromHexString("0x1");
    final Address contract = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    final BonsaiAccount contractAccount = createAccount(contract, 1, 0);
    for (final BonsaiWorldStateUpdateAccumulator accumulator :
        new BonsaiWorldStateUpdateAccumulator[] {bonsaiUpdater, predecessorUpdater, trxUpdater}) {
      accumulator
          .getAccountsToUpdate()
          .put(contract, new BonsaiValue<>(contractAccount, contractAccount));
      accumulator
          .getAccountsToUpdate()
          .put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    }
    putSlot(bonsaiUpdater, contract, 7, null, UInt256.ZERO);
    putSlot(trxUpdater, contract, 7, null, null);

    assertFalse(
        hasChainedCollision(createTransaction(sender, contract)),
        "Expected no collision when an absent slot was written as zero");
  }

  @Test
  void chainedCheckDoesNotLoadASlotTheBlockOnlyRead() {
    final Address sender = Address.fromHexString("0x1");
    final Address contract = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    final BonsaiAccount contractAccount = createAccount(contract, 1, 0);
    for (final BonsaiWorldStateUpdateAccumulator accumulator :
        new BonsaiWorldStateUpdateAccumulator[] {bonsaiUpdater, predecessorUpdater, trxUpdater}) {
      accumulator
          .getAccountsToUpdate()
          .put(contract, new BonsaiValue<>(contractAccount, contractAccount));
      accumulator
          .getAccountsToUpdate()
          .put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    }
    final Supplier<UInt256> notLoaded =
        () -> {
          throw new AssertionError("loaded a slot the block only read");
        };
    bonsaiUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            contract,
            __ -> new StorageConsumingMap<>(contract, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(new StorageSlotKey(UInt256.valueOf(7)), BonsaiValue.withLazy(notLoaded, notLoaded));
    putSlot(trxUpdater, contract, 7, UInt256.valueOf(4), UInt256.valueOf(4));

    assertFalse(
        hasChainedCollision(createTransaction(sender, contract)),
        "Expected no collision when the block only read the slot");
  }

  @Test
  void chainedTransactionCollidesWhenBlockClearedStorageItTouched() {
    final Address sender = Address.fromHexString("0x1");
    final Address contract = Address.fromHexString("0x2");
    final BonsaiAccount senderAccount = createAccount(sender, 0, 100);
    final BonsaiAccount contractAccount = createAccount(contract, 1, 0);
    for (final BonsaiWorldStateUpdateAccumulator accumulator :
        new BonsaiWorldStateUpdateAccumulator[] {bonsaiUpdater, predecessorUpdater, trxUpdater}) {
      accumulator
          .getAccountsToUpdate()
          .put(contract, new BonsaiValue<>(contractAccount, contractAccount));
      accumulator
          .getAccountsToUpdate()
          .put(sender, new BonsaiValue<>(senderAccount, senderAccount));
    }
    bonsaiUpdater.getStorageToClear().add(contract);

    assertTrue(
        hasChainedCollision(createTransaction(sender, contract)),
        "Expected a collision when the block cleared a storage the transaction touched");
  }

  private boolean hasCollisionCarryingCredits(
      final Transaction transaction, final Map<Address, CarriedAccount> carriedAccounts) {
    return collisionDetector.hasCollision(
        transaction,
        Address.ZERO,
        new ParallelizedTransactionContext(trxUpdater, null, false, Wei.ZERO),
        bonsaiUpdater,
        carriedAccounts);
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
    final Map<Address, CarriedAccount> carried = new HashMap<>();

    assertFalse(hasCollisionCarryingCredits(creditScenario(55), carried));
    assertThat(carried).containsOnlyKeys(Address.fromHexString("0x2"));
  }

  @Test
  void creditToAnAccountWhoseBalanceWasObservedIsACollision() {
    final Transaction transaction = creditScenario(55);
    trxUpdater.observeBalance(Address.fromHexString("0x2"));

    assertTrue(hasCollisionCarryingCredits(transaction, new HashMap<>()));
  }

  @Test
  void spendingFromAnAccountWhoseBalanceChangedIsACollision() {
    assertTrue(hasCollisionCarryingCredits(creditScenario(45), new HashMap<>()));
  }

  @Test
  void spendingWithinItsMarginFromAnAccountWhoseBalanceChangedIsCarriedOver() {
    final Transaction transaction = creditScenario(45);
    // the transaction found 50 where it spent 10
    trxUpdater.observeSufficientBalance(Address.fromHexString("0x2"), Wei.of(40));
    final Map<Address, CarriedAccount> carried = new HashMap<>();

    assertFalse(hasCollisionCarryingCredits(transaction, carried));
    assertThat(carried).containsOnlyKeys(Address.fromHexString("0x2"));
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

    assertTrue(hasCollisionCarryingCredits(createTransaction(sender, spent), new HashMap<>()));
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
            new HashMap<>()));
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

    assertTrue(hasCollisionCarryingCredits(createTransaction(sender, credited), new HashMap<>()));
  }

  private Transaction senderScenario(final long blockBalance) {
    final Address sender = Address.fromHexString("0x1");
    trxUpdater.recordBalanceObservations();
    // an earlier transaction of the block changed the sender's balance
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(
            sender,
            new BonsaiValue<>(
                createAccount(sender, 1, 50), createAccount(sender, 1, blockBalance)));
    trxUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(createAccount(sender, 1, 50), createAccount(sender, 2, 40)));
    return createTransaction(sender, Address.fromHexString("0x3"));
  }

  @Test
  void senderBalanceTheBlockRaisedIsCarriedOver() {
    final Map<Address, CarriedAccount> carried = new HashMap<>();

    assertFalse(hasCollisionCarryingCredits(senderScenario(70), carried));
    assertThat(carried).containsOnlyKeys(Address.fromHexString("0x1"));
  }

  @Test
  void senderBalanceTheBlockLoweredIsACollision() {
    assertTrue(hasCollisionCarryingCredits(senderScenario(45), new HashMap<>()));
  }

  @Test
  void senderBalanceTheRunObservedIsACollision() {
    final Transaction transaction = senderScenario(70);
    trxUpdater.observeBalance(Address.fromHexString("0x1"));

    assertTrue(hasCollisionCarryingCredits(transaction, new HashMap<>()));
  }

  @Test
  void senderNonceTheBlockChangedIsACollision() {
    final Address sender = Address.fromHexString("0x1");
    trxUpdater.recordBalanceObservations();
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(createAccount(sender, 1, 50), createAccount(sender, 2, 70)));
    trxUpdater
        .getAccountsToUpdate()
        .put(sender, new BonsaiValue<>(createAccount(sender, 1, 50), createAccount(sender, 2, 40)));

    assertTrue(
        hasCollisionCarryingCredits(
            createTransaction(sender, Address.fromHexString("0x3")), new HashMap<>()));
  }

  /**
   * Earlier transactions of the block raised the account's nonce from the given one and its
   * balance. The transaction paid the account and left it with the given nonce.
   */
  private void nonceScenario(
      final Address account, final long parentNonce, final long transactionNonce) {
    trxUpdater.recordBalanceObservations();
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(
            account,
            new BonsaiValue<>(
                createAccount(account, parentNonce, 50),
                createAccount(account, parentNonce + 1, 70)));
    trxUpdater
        .getAccountsToUpdate()
        .put(
            account,
            new BonsaiValue<>(
                createAccount(account, parentNonce, 50),
                createAccount(account, transactionNonce, 55)));
  }

  @Test
  void nonceTheBlockChangedIsKeptForAnAccountTheTransactionPaid() {
    final Address paid = Address.fromHexString("0x2");
    nonceScenario(paid, 1, 1);
    final Map<Address, CarriedAccount> carried = new HashMap<>();

    assertFalse(
        hasCollisionCarryingCredits(
            createTransaction(Address.fromHexString("0x1"), paid), carried));
    assertThat(carried).containsOnlyKeys(paid);
    // the run's credit of 5 on the block's balance of 70, and the block's nonce
    assertThat(carried.get(paid)).isEqualTo(new CarriedAccount(Wei.of(75), 2));
  }

  @Test
  void nonceTheBlockChangedIsACollisionForAnAccountWithCode() {
    // a creation in a frame that reverted read the contract's nonce and left it unchanged
    final Address contract = Address.fromHexString("0x2");
    trxUpdater.recordBalanceObservations();
    bonsaiUpdater
        .getAccountsToUpdate()
        .put(contract, new BonsaiValue<>(createContract(contract, 1), createContract(contract, 2)));
    trxUpdater
        .getAccountsToUpdate()
        .put(contract, new BonsaiValue<>(createContract(contract, 1), createContract(contract, 1)));

    assertTrue(
        hasCollisionCarryingCredits(
            createTransaction(Address.fromHexString("0x1"), contract), new HashMap<>()));
  }

  @Test
  void nonceTheBlockRaisedFromZeroIsACollision() {
    // a creation at the account's address would fail at the transaction's turn but not on the
    // parent state
    final Address paid = Address.fromHexString("0x2");
    nonceScenario(paid, 0, 0);

    assertTrue(
        hasCollisionCarryingCredits(
            createTransaction(Address.fromHexString("0x1"), paid), new HashMap<>()));
  }

  @Test
  void nonceTheBlockChangedIsACollisionWhenTheTransactionChangedItToo() {
    final Address paid = Address.fromHexString("0x2");
    nonceScenario(paid, 1, 2);

    assertTrue(
        hasCollisionCarryingCredits(
            createTransaction(Address.fromHexString("0x1"), paid), new HashMap<>()));
  }

  @Test
  void nonceTheBlockChangedIsACollisionForAnAuthorityOfTheTransaction() {
    final Address authority = Address.fromHexStringStrict(ACCOUNT_GENESIS_2);
    nonceScenario(authority, 1, 1);
    // signed for the nonce the block reached, so the run on the parent state skipped it
    final Transaction transaction =
        new Transaction.Builder()
            .type(TransactionType.DELEGATE_CODE)
            .nonce(1)
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFeePerGas(Wei.ONE)
            .gasLimit(100_000)
            .to(authority)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .chainId(BigInteger.ONE)
            .codeDelegations(
                List.of(
                    CodeDelegation.builder()
                        .chainId(BigInteger.ONE)
                        .address(Address.fromHexString("0x9"))
                        .nonce(2)
                        .signAndBuild(ACCOUNT_GENESIS_2_KEYPAIR)))
            .sender(Address.fromHexString("0x1"))
            .build();

    assertTrue(hasCollisionCarryingCredits(transaction, new HashMap<>()));
  }
}
