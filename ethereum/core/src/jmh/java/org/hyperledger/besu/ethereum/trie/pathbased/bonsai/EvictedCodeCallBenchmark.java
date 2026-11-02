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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
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

/**
 * Calls more distinct contract code than the code cache holds, several times per block.
 *
 * <p>Each target jumps over a body of random {@code STOP}, {@code JUMPDEST} and {@code PUSH1} bytes
 * to its last {@code JUMPDEST}, so a call costs little gas but needs the jump destination analysis
 * of the whole code. One benchmark operation is one block: a new accumulator over the same state
 * and code cache, in which a controller calls every target {@link #ROUNDS} times. With a cache
 * smaller than the targets, a block evicts code it calls again.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EvictedCodeCallBenchmark {

  private static final int CONTRACTS = 1024;
  private static final int CODE_SIZE = 48 * 1024;
  private static final int ROUNDS = 5;
  private static final Address FIRST_TARGET =
      Address.fromHexString("0x00000000000000000000000000000000c0de0000");
  private static final Address CONTROLLER =
      Address.fromHexString("0x00000000000000000000000000000000c0ffee00");

  // the targets weigh ~54 MiB in the cache
  @Param({"16", "128"})
  public int codeCacheMiB;

  private BonsaiWorldState worldState;
  private EVMExecutor executor;

  @Setup(Level.Trial)
  public void setUp() {
    final BonsaiWorldStateProvider archive =
        new BonsaiWorldStateProvider(
            (BonsaiWorldStateKeyValueStorage)
                new InMemoryKeyValueStorageProvider()
                    .createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            mock(Blockchain.class),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getExtraStorageConfiguration(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache(codeCacheMiB * 1024L * 1024L));
    worldState = (BonsaiWorldState) archive.getWorldState();

    final Random random = new Random(42);
    final WorldUpdater setup = worldState.updater();
    for (int i = 0; i < CONTRACTS; i++) {
      setup.createAccount(target(i), 1, Wei.ZERO).setCode(targetCode(i, random));
    }
    setup.commit();
    worldState.persist(null);

    startBlock();
    final CallCounter callCounter = new CallCounter();
    executor =
        new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.AMSTERDAM))
            .code(controllerCode())
            .receiver(CONTROLLER)
            .gas(1_000_000_000_000L)
            .tracer(callCounter);
    callEveryTarget();
    if (callCounter.successfulCalls != CONTRACTS * ROUNDS) {
      throw new IllegalStateException(
          "Expected " + CONTRACTS * ROUNDS + " calls, got " + callCounter.successfulCalls);
    }
    executor.tracer(OperationTracer.NO_TRACING);
  }

  @Setup(Level.Invocation)
  public void startBlock() {
    // the world state keeps one accumulator, so clear what the previous block loaded into it
    worldState.updater().reset();
  }

  @Benchmark
  public Bytes callEveryTarget() {
    return executor.worldUpdater(worldState.updater()).execute();
  }

  private static Address target(final int index) {
    // FIRST_TARGET ends in two zero bytes, as many as the index needs
    final byte[] address = FIRST_TARGET.getBytes().toArray();
    address[18] = (byte) (index >>> 8);
    address[19] = (byte) index;
    return Address.wrap(Bytes.wrap(address));
  }

  private static Bytes targetCode(final int index, final Random random) {
    final byte[] code = new byte[CODE_SIZE];
    final int lastJumpDest = CODE_SIZE - 2;
    // PUSH2 lastJumpDest, JUMP
    code[0] = 0x61;
    code[1] = (byte) (lastJumpDest >>> 8);
    code[2] = (byte) lastJumpDest;
    code[3] = 0x56;
    // a distinct code hash for every target
    code[4] = (byte) (index >>> 24);
    code[5] = (byte) (index >>> 16);
    code[6] = (byte) (index >>> 8);
    code[7] = (byte) index;
    final byte[] bodyBytes = {0x00, 0x5b, 0x60};
    // the last 33 body bytes stay STOP, so no PUSH covers the last JUMPDEST
    for (int i = 8; i < lastJumpDest - 33; i++) {
      code[i] = bodyBytes[random.nextInt(bodyBytes.length)];
    }
    code[lastJumpDest] = 0x5b;
    return Bytes.wrap(code);
  }

  private static Bytes controllerCode() {
    final ByteArrayOutputStream code = new ByteArrayOutputStream();
    // for rounds = ROUNDS; rounds != 0; rounds--
    code.writeBytes(new byte[] {0x61, (byte) (ROUNDS >>> 8), (byte) ROUNDS});
    final int roundLoop = code.size();
    code.write(0x5b);
    //   for i = CONTRACTS; i != 0; i--: CALL(GAS, FIRST_TARGET + i - 1, 0, 0, 0, 0, 0)
    code.writeBytes(new byte[] {0x61, (byte) (CONTRACTS >>> 8), (byte) CONTRACTS});
    final int callLoop = code.size();
    code.write(0x5b);
    code.writeBytes(new byte[] {0x60, 0x01, (byte) 0x90, 0x03}); // PUSH1 1, SWAP1, SUB
    for (int i = 0; i < 5; i++) {
      code.writeBytes(new byte[] {0x60, 0x00}); // PUSH1 0
    }
    code.write(0x85); // DUP6
    code.write(0x73); // PUSH20
    code.writeBytes(FIRST_TARGET.getBytes().toArrayUnsafe());
    code.writeBytes(new byte[] {0x01, 0x5a, (byte) 0xf1, 0x50}); // ADD, GAS, CALL, POP
    code.writeBytes(new byte[] {(byte) 0x80, 0x61, 0x00, (byte) callLoop, 0x57}); // DUP1, JUMPI
    code.write(0x50); // POP
    code.writeBytes(new byte[] {0x60, 0x01, (byte) 0x90, 0x03}); // PUSH1 1, SWAP1, SUB
    code.writeBytes(new byte[] {(byte) 0x80, 0x61, 0x00, (byte) roundLoop, 0x57}); // DUP1, JUMPI
    code.write(0x00); // STOP
    return Bytes.wrap(code.toByteArray());
  }

  private static final class CallCounter implements OperationTracer {
    private int successfulCalls;

    @Override
    public void traceContextExit(final MessageFrame frame) {
      if (frame.getDepth() == 1 && frame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
        successfulCalls++;
      }
    }
  }
}
