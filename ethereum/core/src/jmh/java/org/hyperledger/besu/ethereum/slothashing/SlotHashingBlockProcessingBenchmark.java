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
package org.hyperledger.besu.ethereum.slothashing;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.parallelization.MainnetParallelBlockProcessor;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Imports a block on Bonsai where every transaction reads the same hot slots and writes slots of
 * its own, before and after Amsterdam, sequentially and in parallel. With a block access list, the
 * list is decoded from RLP as when the block comes from the network. Besides the time per block, it
 * prints the keccak256 computed per block, counted on every thread by {@link KeccakCounter}.
 *
 * <p>Run with {@code ./gradlew :ethereum:core:jmh -Pincludes=SlotHashingBlockProcessingBenchmark}.
 */
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Djdk.attach.allowAttachSelf=true", "-XX:+EnableDynamicAgentLoading"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SlotHashingBlockProcessingBenchmark {

  /** Fork and execution path. */
  public enum Scenario {
    OSAKA_SEQUENTIAL(false, false, false),
    OSAKA_PARALLEL(false, true, false),
    AMSTERDAM_SEQUENTIAL(true, false, false),
    AMSTERDAM_PARALLEL(true, true, false),
    AMSTERDAM_BAL_PARALLEL(true, true, true);

    final boolean amsterdam;
    final boolean parallel;
    final boolean withBal;

    Scenario(final boolean amsterdam, final boolean parallel, final boolean withBal) {
      this.amsterdam = amsterdam;
      this.parallel = parallel;
      this.withBal = withBal;
    }
  }

  @Param({
    "OSAKA_SEQUENTIAL",
    "OSAKA_PARALLEL",
    "AMSTERDAM_SEQUENTIAL",
    "AMSTERDAM_PARALLEL",
    "AMSTERDAM_BAL_PARALLEL"
  })
  public Scenario scenario;

  /** Transactions per block, each from its own sender. */
  @Param({"64"})
  public int transactions;

  /** Hot slots read by every transaction. */
  @Param({"64"})
  public int sharedReadSlots;

  /** Slots written by each transaction, different from one transaction to another. */
  @Param({"16"})
  public int ownWriteSlots;

  private final KeccakPerBlock keccak = new KeccakPerBlock();
  private SlotHashingChain chain;
  private BlockProcessor processor;
  private Block block;
  private Optional<Bytes> blockAccessListRlp;
  private BonsaiWorldState worldState;

  @Setup(Level.Trial)
  public void setUp() {
    KeccakCounter.install();
    chain = new SlotHashingChain(scenario.amsterdam, transactions, sharedReadSlots, ownWriteSlots);
    final BlockCreationResult created = chain.createBlock();
    block = created.getBlock();
    blockAccessListRlp =
        scenario.withBal
            ? Optional.of(created.getBlockAccessList().orElseThrow().encode())
            : Optional.empty();

    final ProtocolSpec spec =
        chain.context().getProtocolSchedule().getByBlockHeader(block.getHeader());
    processor =
        scenario.parallel
            ? new MainnetParallelBlockProcessor(
                spec.getTransactionProcessor(),
                spec.getTransactionReceiptFactory(),
                Wei.ZERO,
                BlockHeader::getCoinbase,
                true,
                chain.context().getProtocolSchedule(),
                BalConfiguration.DEFAULT,
                new NoOpMetricsSystem())
            : new MainnetBlockProcessor(
                spec.getTransactionProcessor(),
                spec.getTransactionReceiptFactory(),
                Wei.ZERO,
                BlockHeader::getCoinbase,
                true,
                chain.context().getProtocolSchedule(),
                BalConfiguration.DEFAULT);

    freshWorldState();
    keccak.findDistinctSlots(this::checkImport);
    closeWorldState();
  }

  @Setup(Level.Invocation)
  public void freshWorldState() {
    worldState = chain.genesisWorldState();
  }

  @TearDown(Level.Invocation)
  public void closeWorldState() {
    worldState.close();
  }

  @Benchmark
  public BlockProcessingResult importBlock() {
    return keccak.count(this::processBlock);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    keccak.print(scenario + " block import");
    chain.close();
  }

  /** Decodes the block access list received with the block, if any, then processes the block. */
  private BlockProcessingResult processBlock() {
    return processor.processBlock(
        chain.context().getProtocolContext(),
        chain.context().getBlockchain(),
        worldState,
        block,
        blockAccessListRlp.map(BlockAccessList::fromBytes));
  }

  private void checkImport() {
    final BlockProcessingResult result = processBlock();
    if (!result.isSuccessful()) {
      throw new IllegalStateException(scenario + " fails: " + result.errorMessage);
    }
  }
}
