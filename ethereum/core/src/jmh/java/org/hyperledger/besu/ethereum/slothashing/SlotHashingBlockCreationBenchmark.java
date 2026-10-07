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

import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;

import java.util.concurrent.TimeUnit;

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
 * Builds the block of {@link SlotHashingBlockProcessingBenchmark} on Bonsai, as a block producer
 * does, before and after Amsterdam. Besides the time per block, it prints the keccak256 computed
 * per block, counted on every thread by {@link KeccakCounter}.
 *
 * <p>Run with {@code ./gradlew :ethereum:core:jmh -Pincludes=SlotHashingBlockCreationBenchmark}.
 */
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Djdk.attach.allowAttachSelf=true", "-XX:+EnableDynamicAgentLoading"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SlotHashingBlockCreationBenchmark {

  /** Fork of the block, Amsterdam builds a block access list. */
  public enum Scenario {
    OSAKA,
    AMSTERDAM
  }

  @Param({"OSAKA", "AMSTERDAM"})
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

  @Setup(Level.Trial)
  public void setUp() {
    KeccakCounter.install();
    chain =
        new SlotHashingChain(
            scenario == Scenario.AMSTERDAM, transactions, sharedReadSlots, ownWriteSlots);
    keccak.findDistinctSlots(chain::createBlock);
  }

  @Benchmark
  public BlockCreationResult createBlock() {
    return keccak.count(chain::createBlock);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    keccak.print(scenario + " block creation");
    chain.close();
  }
}
