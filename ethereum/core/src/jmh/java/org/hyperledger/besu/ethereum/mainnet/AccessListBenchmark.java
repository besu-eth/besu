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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.util.AddressStorageSlotKeyHashing;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures the cost of unpacking an EIP-2930 access list from a transaction payload into the warm
 * storage set.
 *
 * <p>The benchmarked workload is a plain value transfer to an account with no code. That is the
 * minimal transaction that still carries an access list, so the only work that scales with {@link
 * #slotCount} is the decode loop in {@code MainnetTransactionProcessor.processTransaction} that
 * builds the {@code HashSet<AddressStorageSlotKey>}, plus the bulk {@code
 * MessageFrame.warmUpStorage(Set)} that drains it into the frame. The transfer executes no opcodes,
 * so nothing else in the measurement grows with the parameter.
 *
 * <p>All slots are accessed from a single access-list address, which is the worst case of an access
 * list in a transaction payload. This benchmark tests that hashcode collisions upon insertion do
 * not degrade into O(n^2) while creating the access list and passing it downstream to the eip-2929
 * storage warmup internals. If the hashing algorithm changes, this benchmark will have to change
 * accordingly to produce hashcode collisions properly.
 *
 * <p>Berlin is chosen deliberately as it is the earliest fork carrying the EIP-2930 schedule.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class AccessListBenchmark {

  /** Gas limit large enough to cover the intrinsic cost of the biggest access list benchmarked. */
  private static final long GAS_LIMIT = 400_000_000L;

  private static final BigInteger CHAIN_ID = BigInteger.ONE;

  private static final Address ACCESS_LIST_ADDRESS =
      Address.fromHexString("0xbb00000000000000000000000000000000000000");
  private static final Address RECIPIENT =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address COINBASE =
      Address.fromHexString("0x4242424242424242424242424242424242424242");

  /** How the storage keys are chosen, relative to the seeded key hash. */
  public enum SlotHashing {
    /** Every key hashes to the same value. */
    COLLIDING,
    /** Keys are spread over the whole int range. */
    DISTINCT,
    /** Keys are uniformly random, the typical usage of an access list. */
    RANDOM
  }

  @Param({"10000", "50000", "150000"})
  public int slotCount;

  @Param public SlotHashing slotHashing;

  private MainnetTransactionProcessor transactionProcessor;
  private Transaction transaction;
  private MutableWorldState worldState;
  private BlockHeader blockHeader;
  private BlockHashLookup blockHashLookup;

  private WorldUpdater worldUpdater;

  @Setup
  public void setUp() throws Exception {
    final KeyPair keyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    transaction = buildTransfer(keyPair, buildAccessList());

    blockHeader =
        new BlockHeaderTestFixture()
            .number(1)
            .gasLimit(GAS_LIMIT)
            .coinbase(COINBASE)
            .baseFeePerGas(Wei.ZERO)
            .buildHeader();

    final ProtocolSchedule protocolSchedule =
        MainnetProtocolSchedule.fromConfig(
            new StubGenesisConfigOptions().berlinBlock(0).chainId(CHAIN_ID),
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            ImmutableBalConfiguration.builder().build(),
            new NoOpMetricsSystem());
    transactionProcessor = protocolSchedule.getByBlockHeader(blockHeader).getTransactionProcessor();

    // Real Forest world state over an in-memory key-value store, with the sender funded so the
    // real validator accepts the transaction.
    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive().getWorldState();
    final WorldUpdater setupUpdater = worldState.updater();
    setupUpdater.createAccount(transaction.getSender(), 0, Wei.fromEth(1_000));
    setupUpdater.commit();
    worldState.persist(null);

    blockHashLookup = (__, ___) -> Hash.ZERO;
  }

  /**
   * A fresh updater per invocation. The transaction bumps the sender nonce, so without this the
   * real validator would reject every call after the first. Nothing is committed back to {@link
   * #worldState}, so every invocation starts from the same funded state. The cost of this operation
   * is just the cost of running a constructor so it doesn't impact benchmarks that much.
   */
  @Setup(Level.Invocation)
  public void setUpInvocation() {
    worldUpdater = worldState.updater();
  }

  private List<AccessListEntry> buildAccessList() throws Exception {
    final Random random = new Random(34223243L);
    final List<Bytes32> slots = new ArrayList<>(slotCount);
    for (int i = 0; i < slotCount; i++) {
      slots.add(
          switch (slotHashing) {
            case RANDOM -> Bytes32.random(random);
            case COLLIDING -> AddressStorageSlotKeyHashing.collidingHash(ACCESS_LIST_ADDRESS, i);
            case DISTINCT -> AddressStorageSlotKeyHashing.distinctHash(ACCESS_LIST_ADDRESS, i);
          });
    }
    return List.of(new AccessListEntry(ACCESS_LIST_ADDRESS, slots));
  }

  private Transaction buildTransfer(final KeyPair keyPair, final List<AccessListEntry> accessList) {
    return Transaction.builder()
        .type(TransactionType.ACCESS_LIST)
        .chainId(CHAIN_ID)
        .nonce(0)
        .gasPrice(Wei.ZERO)
        .gasLimit(GAS_LIMIT)
        .to(RECIPIENT)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .accessList(accessList)
        .signAndBuild(keyPair);
  }

  @Benchmark
  public void ethTransfer(final Blackhole blackhole) {
    blackhole.consume(
        transactionProcessor.processTransaction(
            worldUpdater,
            blockHeader,
            transaction,
            COINBASE,
            blockHashLookup,
            TransactionValidationParams.processingBlock(),
            Wei.ZERO));
  }
}
