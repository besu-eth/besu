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
package org.hyperledger.besu.ethereum.vm;

import org.hyperledger.besu.evm.Code;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the jump destination analysis that a {@link Code} runs on its first JUMP, across code
 * shapes that stress it differently.
 *
 * <p>Each invocation builds a new {@link Code}, because the analysis is memoised per instance: code
 * that misses every cache, such as initcode or a contract called for the first time, pays the full
 * analysis on its first JUMP, and its bytes are chosen by whoever deployed it.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class CodeJumpDestAnalysisBenchmark {

  private static final int STOP = 0x00;
  private static final int ADD = 0x01;
  private static final int JUMPDEST = 0x5b;
  private static final int PUSH1 = 0x60;
  private static final int PUSH32 = 0x7f;
  private static final int MAX_CODE_SIZE_AMSTERDAM = 65536;
  private static final int MAX_CODE_SIZE_MAINNET = 24576;

  /**
   * Random shapes get several instances, so that the branch predictor cannot learn a single body
   * across invocations.
   */
  private static final int INSTANCES = 8;

  /**
   * ATTACK_ALL_JUMPDEST: the glamsterdam devnet-8 spam contract. `PUSH2 0xffff; JUMP` followed by
   * 64 KiB of JUMPDEST, with the contract's own address embedded so that every deployed copy has a
   * distinct code hash.
   *
   * <p>PUSH1_JUMPDEST_PERIODIC: `PUSH1 0x5b` repeated, every other byte a JUMPDEST that is PUSH
   * data.
   *
   * <p>PUSH32_DENSE: a chain of PUSH32, almost every byte immediate data.
   *
   * <p>MIXED: arithmetic, small pushes and a JUMPDEST every 32 bytes, closer to compiled output.
   *
   * <p>RANDOM_*: bytes drawn uniformly from the listed opcodes. Every byte is a new, unpredictable
   * decision for an analysis that branches on the opcode. STOP_JUMPDEST_PUSH1 and JUMPDEST_PUSH1
   * are the bodies of the jump destination analysis benchmarks in ethereum/execution-specs#3631.
   *
   * <p>RANDOM_TWELVE_PUSH1_PER_CHUNK: RANDOM_STOP_JUMPDEST with twelve PUSH1 in every 64 bytes, at
   * random positions.
   *
   * <p>SOLIDITY_13K: a real compiled Solidity contract from a devnet, 13,199 bytes with 2,378 PUSH
   * and 465 JUMPDEST instructions. The same bytes are analysed on every invocation, so an analysis
   * that branches on the opcode runs with a branch predictor trained on this very contract.
   *
   * <p>SOLIDITY_OPCODE_MIX: random programs of 24 KiB drawn from the instructions of SOLIDITY_13K,
   * with random immediates. It has the instruction mix of compiled code without repeating, as code
   * seen for the first time does not.
   *
   * <p>SMALL: MIXED at 256 bytes.
   */
  @Param({
    "ATTACK_ALL_JUMPDEST",
    "PUSH1_JUMPDEST_PERIODIC",
    "PUSH32_DENSE",
    "MIXED",
    "RANDOM_STOP_JUMPDEST",
    "RANDOM_STOP_JUMPDEST_PUSH1",
    "RANDOM_JUMPDEST_PUSH1",
    "RANDOM_ADD_JUMPDEST_PUSH1_TO_PUSH4",
    "RANDOM_BYTES",
    "RANDOM_TWELVE_PUSH1_PER_CHUNK",
    "SOLIDITY_13K",
    "SOLIDITY_OPCODE_MIX",
    "SMALL"
  })
  private String caseName;

  private Bytes[] codes;
  private int next;

  @Setup
  public void setUp() {
    codes = new Bytes[INSTANCES];
    final Random random = new Random(caseName.hashCode());
    for (int i = 0; i < INSTANCES; i++) {
      codes[i] = Bytes.wrap(shape(caseName, random));
    }
  }

  /**
   * Every fork sees a single shape, so without this the JIT would specialise the analysis to that
   * shape alone, which production never gets. Running all shapes first gives it a mixed profile.
   */
  @Setup(Level.Trial)
  public void warmUpAllShapes() {
    final String[] shapes = {
      "ATTACK_ALL_JUMPDEST",
      "PUSH1_JUMPDEST_PERIODIC",
      "PUSH32_DENSE",
      "MIXED",
      "RANDOM_STOP_JUMPDEST",
      "RANDOM_STOP_JUMPDEST_PUSH1",
      "RANDOM_JUMPDEST_PUSH1",
      "RANDOM_ADD_JUMPDEST_PUSH1_TO_PUSH4",
      "RANDOM_BYTES",
      "RANDOM_TWELVE_PUSH1_PER_CHUNK",
      "SOLIDITY_13K",
      "SOLIDITY_OPCODE_MIX",
      "SMALL"
    };
    final Random random = new Random(1);
    final byte[][] bodies = new byte[shapes.length][];
    for (int i = 0; i < shapes.length; i++) {
      bodies[i] = shape(shapes[i], random);
    }
    int sink = 0;
    for (int round = 0; round < 200; round++) {
      for (final byte[] body : bodies) {
        sink += new Code(Bytes.wrap(body)).isJumpDestInvalid(body.length - 1) ? 1 : 0;
      }
    }
    if (sink < 0) {
      throw new IllegalStateException();
    }
  }

  private static byte[] shape(final String name, final Random random) {
    return switch (name) {
      case "ATTACK_ALL_JUMPDEST" -> attackContract();
      case "PUSH1_JUMPDEST_PERIODIC" -> periodic(MAX_CODE_SIZE_MAINNET, PUSH1, JUMPDEST);
      case "PUSH32_DENSE" -> pushDense(MAX_CODE_SIZE_MAINNET);
      case "MIXED" -> mixed(MAX_CODE_SIZE_MAINNET);
      case "RANDOM_STOP_JUMPDEST" -> randomOf(random, STOP, JUMPDEST);
      case "RANDOM_STOP_JUMPDEST_PUSH1" -> randomOf(random, STOP, JUMPDEST, PUSH1);
      case "RANDOM_JUMPDEST_PUSH1" -> randomOf(random, JUMPDEST, PUSH1);
      case "RANDOM_ADD_JUMPDEST_PUSH1_TO_PUSH4" ->
          randomOf(random, ADD, JUMPDEST, PUSH1, PUSH1 + 1, PUSH1 + 2, PUSH1 + 3);
      case "RANDOM_BYTES" -> {
        final byte[] code = new byte[MAX_CODE_SIZE_MAINNET];
        random.nextBytes(code);
        yield code;
      }
      case "RANDOM_TWELVE_PUSH1_PER_CHUNK" -> pushesPerChunk(random, 12);
      case "SOLIDITY_13K" -> solidityContract();
      case "SOLIDITY_OPCODE_MIX" -> opcodeMix(random, solidityContract());
      case "SMALL" -> mixed(256);
      default -> throw new IllegalArgumentException("unknown case " + name);
    };
  }

  /** PUSH2 0xffff; JUMP; own address at offset 44; JUMPDEST everywhere else. */
  private static byte[] attackContract() {
    final byte[] code = new byte[MAX_CODE_SIZE_AMSTERDAM];
    Arrays.fill(code, (byte) JUMPDEST);
    code[0] = 0x61;
    code[1] = (byte) 0xff;
    code[2] = (byte) 0xff;
    code[3] = 0x56;
    final byte[] address =
        Bytes.fromHexString("0x8172765afc45126f1428f6070e749accdc2d7ba0").toArrayUnsafe();
    System.arraycopy(address, 0, code, 44, address.length);
    return code;
  }

  private static byte[] periodic(final int size, final int... pattern) {
    final byte[] code = new byte[size];
    for (int i = 0; i < size; i++) {
      code[i] = (byte) pattern[i % pattern.length];
    }
    return code;
  }

  /** A chain of PUSH32, padded with JUMPDEST. */
  private static byte[] pushDense(final int size) {
    final byte[] code = new byte[size];
    int i = 0;
    while (i + 33 < size) {
      code[i] = (byte) PUSH32;
      i += 33;
    }
    while (i < size) {
      code[i++] = (byte) JUMPDEST;
    }
    return code;
  }

  /** ADD/MUL/PUSH1/PUSH2/DUP with a JUMPDEST every 32 bytes. */
  private static byte[] mixed(final int size) {
    final byte[] pattern = {
      0x60,
      0x01,
      0x60,
      0x02,
      0x01,
      (byte) 0x80,
      0x02,
      0x61,
      0x11,
      0x22,
      0x03,
      (byte) 0x90,
      0x04,
      0x50,
      0x60,
      0x40,
      0x51,
      0x60,
      0x20,
      0x52,
      0x01,
      0x03,
      (byte) 0x80,
      0x50
    };
    final byte[] code = new byte[size];
    for (int i = 0; i < size; i++) {
      code[i] = (i % 32 == 31) ? (byte) JUMPDEST : pattern[i % pattern.length];
    }
    return code;
  }

  /**
   * Random STOP and JUMPDEST, with the given number of PUSH1 per 64 bytes at random even offsets.
   */
  private static byte[] pushesPerChunk(final Random random, final int pushes) {
    final byte[] code = randomOf(random, STOP, JUMPDEST);
    final int[] offsets = new int[32];
    for (int chunkStart = 0; chunkStart + 64 <= code.length; chunkStart += 64) {
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] = 2 * i;
      }
      for (int i = offsets.length - 1; i > 0; i--) {
        final int j = random.nextInt(i + 1);
        final int swap = offsets[i];
        offsets[i] = offsets[j];
        offsets[j] = swap;
      }
      for (int i = 0; i < pushes; i++) {
        code[chunkStart + offsets[i]] = (byte) PUSH1;
      }
    }
    return code;
  }

  private static byte[] solidityContract() {
    try (InputStream in =
        CodeJumpDestAnalysisBenchmark.class.getResourceAsStream("solidity-contract-13k.hex")) {
      return Bytes.fromHexString(new String(in.readAllBytes(), StandardCharsets.US_ASCII).trim())
          .toArrayUnsafe();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Instructions drawn at random from those of the given code, each with random immediates. */
  private static byte[] opcodeMix(final Random random, final byte[] source) {
    final List<Integer> opcodes = new ArrayList<>();
    for (int i = 0; i < source.length; ) {
      final int opcode = source[i] & 0xff;
      opcodes.add(opcode);
      i += opcode >= PUSH1 && opcode <= PUSH32 ? opcode - PUSH1 + 2 : 1;
    }
    final byte[] code = new byte[MAX_CODE_SIZE_MAINNET];
    for (int i = 0; i < code.length; ) {
      final int opcode = opcodes.get(random.nextInt(opcodes.size()));
      code[i++] = (byte) opcode;
      if (opcode >= PUSH1 && opcode <= PUSH32) {
        for (int j = opcode - PUSH1 + 1; j > 0 && i < code.length; j--) {
          code[i++] = (byte) random.nextInt(256);
        }
      }
    }
    return code;
  }

  private static byte[] randomOf(final Random random, final int... opcodes) {
    final byte[] code = new byte[MAX_CODE_SIZE_MAINNET];
    for (int i = 0; i < code.length; i++) {
      code[i] = (byte) opcodes[random.nextInt(opcodes.length)];
    }
    return code;
  }

  /**
   * Analyses the next instance from scratch, through the check a JUMP to its last byte makes.
   *
   * @return whether the destination is invalid
   */
  @Benchmark
  public boolean coldAnalysis() {
    final Bytes code = codes[next];
    next = (next + 1) % INSTANCES;
    return new Code(code).isJumpDestInvalid(code.size() - 1);
  }
}
