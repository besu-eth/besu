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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.Optional;
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
            new CodeCache());
    trxUpdater =
        new BonsaiWorldStateUpdateAccumulator(
            worldState,
            (__, ___) -> {},
            (__, ___) -> {},
            EvmConfiguration.DEFAULT,
            new CodeCache());
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
        Hash.EMPTY_TRIE_HASH,
        Hash.EMPTY,
        false,
        new CodeCache());
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
        .put(address, new PathBasedValue<>(priorAccountValue, nextAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new PathBasedValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
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
        .put(address, new PathBasedValue<>(priorAccountValue, nextAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new PathBasedValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
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
        .put(address, new PathBasedValue<>(priorAccountValue, nextAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new PathBasedValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
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
        .put(address, new PathBasedValue<>(priorAccountValue, nextAccountValue));
    bonsaiUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(updateStorageSlotKey, new PathBasedValue<>(UInt256.ONE, UInt256.ZERO));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new PathBasedValue<>(priorAccountValue, priorAccountValue));
    trxUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(updateStorageSlotKey, new PathBasedValue<>(UInt256.ONE, UInt256.ONE));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
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
        .put(address, new PathBasedValue<>(priorAccountValue, nextAccountValue));
    bonsaiUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(new StorageSlotKey(UInt256.ZERO), new PathBasedValue<>(UInt256.ONE, UInt256.ZERO));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new PathBasedValue<>(priorAccountValue, priorAccountValue));
    trxUpdater
        .getStorageToUpdate()
        .computeIfAbsent(
            address,
            __ -> new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), (___, ____) -> {}))
        .put(new StorageSlotKey(UInt256.ONE), new PathBasedValue<>(UInt256.ONE, UInt256.ONE));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
            bonsaiUpdater);

    assertFalse(
        hasCollision,
        "Expected no collision when storage roots are modified but different slots are updated.");
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
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
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
        .put(
            miningBeneficiary,
            new PathBasedValue<>(miningBeneficiaryValue, miningBeneficiaryValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            miningBeneficiary,
            new ParallelizedTransactionContext(
                trxUpdater, null, true, Wei.ZERO, Optional.empty(), Optional.empty()),
            bonsaiUpdater);

    assertTrue(hasCollision, "Expected collision with the read mining beneficiary address");
  }

  @Test
  void testCollisionWithDeletedAddress() {
    final Address address = Address.fromHexString("0x1");
    final BonsaiAccount accountValue = createAccount(address);

    // Simulate that the address was deleted in the block
    bonsaiUpdater.getAccountsToUpdate().put(address, new PathBasedValue<>(accountValue, null));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the deleted address is read in the next transaction
    trxUpdater.getAccountsToUpdate().put(address, new PathBasedValue<>(accountValue, accountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
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
        .put(address, new PathBasedValue<>(priorAccountValue, priorAccountValue));

    final Transaction transaction = createTransaction(address, address);

    // Simulate that the address is read in the next transaction
    trxUpdater
        .getAccountsToUpdate()
        .put(address, new PathBasedValue<>(priorAccountValue, priorAccountValue));

    boolean hasCollision =
        collisionDetector.hasCollision(
            transaction,
            Address.ZERO,
            new ParallelizedTransactionContext(
                trxUpdater, null, false, Wei.ZERO, Optional.empty(), Optional.empty()),
            bonsaiUpdater);

    assertFalse(hasCollision, "Expected no collision with the read address");
  }
}
