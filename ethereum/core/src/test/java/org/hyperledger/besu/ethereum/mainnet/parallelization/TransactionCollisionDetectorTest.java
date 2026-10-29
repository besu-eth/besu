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
        bonsaiUpdater);
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
}
