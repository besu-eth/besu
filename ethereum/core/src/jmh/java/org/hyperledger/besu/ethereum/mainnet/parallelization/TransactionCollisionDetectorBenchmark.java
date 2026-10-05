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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.trie.common.MptStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The conflict check of a speculative result against the block, for a transaction that touches a
 * contract whose storage earlier transactions of the block changed in other slots.
 */
@State(Scope.Thread)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TransactionCollisionDetectorBenchmark {

  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000c0ffee01");
  private static final Address OTHER_CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000c0ffee02");
  private static final Address SENDER =
      Address.fromHexString("0x00000000000000000000000000000000000a11ce");
  private static final Address COINBASE =
      Address.fromHexString("0x00000000000000000000000000000000000c0b1e");

  /** Slots of the contract that earlier transactions of the block changed. */
  @Param({"10", "100", "1000"})
  public int blockSlots;

  private final TransactionCollisionDetector detector = new TransactionCollisionDetector();
  private BonsaiWorldStateUpdateAccumulator blockAccumulator;
  private ParallelizedTransactionContext context;
  private Transaction transaction;

  @Setup(Level.Trial)
  public void setUp() {
    final ExecutionContextTestFixture fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource("/genesis-jmh.json"))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final BonsaiWorldState worldState =
        (BonsaiWorldState) fixture.getStateArchive().getWorldState();
    blockAccumulator = accumulator(worldState);
    final BonsaiWorldStateUpdateAccumulator transactionAccumulator = accumulator(worldState);

    final BonsaiAccount contract = account(worldState, CONTRACT, 1, 0);
    final BonsaiAccount otherContract = account(worldState, OTHER_CONTRACT, 1, 0);
    blockAccumulator.getAccountsToUpdate().put(CONTRACT, new BonsaiValue<>(contract, contract));
    // senders of earlier transactions of the block
    for (int i = 0; i < 100; i++) {
      final Address sender = Address.fromHexString(String.format("0x%040x", 0x5e000 + i));
      blockAccumulator
          .getAccountsToUpdate()
          .put(
              sender,
              new BonsaiValue<>(
                  account(worldState, sender, 0, 1_000_000), account(worldState, sender, 1, 900)));
    }
    for (int i = 0; i < blockSlots; i++) {
      slot(blockAccumulator, CONTRACT, 1_000 + i, UInt256.valueOf(i), UInt256.valueOf(i + 1));
    }

    final BonsaiAccount sender = account(worldState, SENDER, 0, 1_000_000);
    transactionAccumulator
        .getAccountsToUpdate()
        .put(SENDER, new BonsaiValue<>(sender, account(worldState, SENDER, 1, 900)));
    transactionAccumulator
        .getAccountsToUpdate()
        .put(CONTRACT, new BonsaiValue<>(contract, contract));
    transactionAccumulator
        .getAccountsToUpdate()
        .put(OTHER_CONTRACT, new BonsaiValue<>(otherContract, otherContract));
    for (int i = 0; i < 6; i++) {
      slot(transactionAccumulator, CONTRACT, i, UInt256.ONE, UInt256.valueOf(2));
    }
    for (int i = 0; i < 4; i++) {
      slot(transactionAccumulator, OTHER_CONTRACT, i, UInt256.ONE, UInt256.ONE);
    }
    context = new ParallelizedTransactionContext(transactionAccumulator, null, false, Wei.ZERO);
    transaction =
        new Transaction.Builder()
            .nonce(0)
            .gasPrice(Wei.of(1))
            .gasLimit(100_000)
            .to(CONTRACT)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .chainId(BigInteger.ONE)
            .sender(SENDER)
            .build();
    if (detector.hasCollision(transaction, COINBASE, context, blockAccumulator)) {
      throw new IllegalStateException("the scenario must not collide");
    }
  }

  private static BonsaiWorldStateUpdateAccumulator accumulator(final BonsaiWorldState worldState) {
    return new BonsaiWorldStateUpdateAccumulator(
        worldState,
        (__, ___) -> {},
        (__, ___) -> {},
        EvmConfiguration.DEFAULT,
        new BonsaiCodeCache());
  }

  private static BonsaiAccount account(
      final BonsaiWorldState worldState,
      final Address address,
      final long nonce,
      final long balance) {
    return new BonsaiAccount(
        worldState,
        address,
        address.addressHash(),
        nonce,
        Wei.of(balance),
        new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
        Hash.EMPTY,
        false,
        new BonsaiCodeCache());
  }

  private static void slot(
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

  @Benchmark
  public boolean hasCollision() {
    return detector.hasCollision(transaction, COINBASE, context, blockAccumulator);
  }

  /** Taking over the speculative result once it does not conflict. */
  @Benchmark
  public void importStateChanges() {
    blockAccumulator.importStateChangesFromSource(
        (BonsaiWorldStateUpdateAccumulator) context.transactionAccumulator());
  }
}
