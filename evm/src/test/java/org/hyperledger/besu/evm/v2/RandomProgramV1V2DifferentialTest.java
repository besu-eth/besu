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
package org.hyperledger.besu.evm.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.v2.ProgramRun.CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.CONTRACT;
import static org.hyperledger.besu.evm.v2.ProgramRun.EMPTY_ACCOUNT;
import static org.hyperledger.besu.evm.v2.ProgramRun.SECOND_CALLEE;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.ProgramRun.Fork;
import org.hyperledger.besu.evm.v2.ProgramRun.Outcome;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/**
 * Runs random programs, which call and create each other, on the v1 interpreter and on EVM v2 and
 * requires both to end in the same state. {@link EveryOpcodeV1V2DifferentialTest} covers the
 * opcodes one at a time; this covers how they work together through memory, storage, return data,
 * logs and nested frames.
 */
class RandomProgramV1V2DifferentialTest {

  private static final int PROGRAMS_PER_FORK = 300;
  private static final long GAS = 3_000_000L;

  static final List<Fork> FORKS = ProgramRun.FORKS;

  /** Operand values, chosen to hit the edges of offsets, sizes, indices, shifts and addresses. */
  private static final List<Bytes> OPERANDS =
      List.of(
          Bytes.of(0),
          Bytes.of(1),
          Bytes.of(2),
          Bytes.of(7),
          Bytes.of(31),
          Bytes.of(32),
          Bytes.of(33),
          Bytes.of(0x40),
          Bytes.of(0xff),
          Bytes.of(0x01, 0x00),
          Bytes.of(0x01, 0x01),
          Bytes.of(0x10, 0x00),
          Bytes.fromHexString("0xffffffff"),
          Bytes.fromHexString("0x0100000000"),
          Bytes.fromHexString("0x7fffffffffffffff"),
          Bytes.fromHexString("0xffffffffffffffff"),
          Bytes.fromHexString("0x010000000000000000"),
          Bytes32.ZERO.not(),
          Bytes.concatenate(Bytes.of(0x80), Bytes.wrap(new byte[31])),
          Bytes32.fromHexString(
              "0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0"),
          CONTRACT.getBytes(),
          CALLEE.getBytes(),
          SECOND_CALLEE.getBytes(),
          EMPTY_ACCOUNT.getBytes());

  /** Operands that make the operations of a program likely to meet in memory and storage. */
  private static final List<Bytes> COMMON_OPERANDS =
      List.of(Bytes.of(0), Bytes.of(1), Bytes.of(2), Bytes.of(32));

  /** The operations that leave state behind for, or read state left by, other operations. */
  private static final List<Integer> STATEFUL_OPCODES =
      List.of(
          0x20, 0x31, 0x35, 0x37, 0x39, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x47, 0x51, 0x52, 0x53, 0x54,
          0x55, 0x5c, 0x5d, 0x5e, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5,
          0xfa, 0xfd);

  /** The storage and transient storage slots the operands can address. */
  private static final List<Bytes32> SLOTS = OPERANDS.stream().map(Bytes32::leftPad).toList();

  @ParameterizedTest(name = "{0}")
  @FieldSource("FORKS")
  void randomProgramsEndInTheSameStateOnV1AndV2(final Fork fork) {
    final EVM v1 = fork.factory().apply(ProgramRun.V1);
    final EVM v2 = fork.factory().apply(ProgramRun.V2);
    final Operation[] operations = v1.getOperationsUnsafe();
    final List<Integer> opcodes = new ArrayList<>();
    for (int opcode = 0; opcode < 0x100; opcode++) {
      if (!(operations[opcode] instanceof InvalidOperation)) {
        opcodes.add(opcode);
      }
    }
    final List<Integer> statefulOpcodes =
        STATEFUL_OPCODES.stream().filter(opcodes::contains).toList();
    final List<String> differences = new ArrayList<>();
    for (int i = 0; i < PROGRAMS_PER_FORK; i++) {
      final Random random = new Random(fork.name().hashCode() * 31L + i);
      final Bytes contract = program(random, opcodes, statefulOpcodes, operations);
      final Bytes callee = program(random, opcodes, statefulOpcodes, operations);
      final Bytes secondCallee = program(random, opcodes, statefulOpcodes, operations);
      final boolean isStatic = random.nextInt(8) == 0;
      final Outcome onV1 = ProgramRun.run(v1, contract, callee, secondCallee, GAS, isStatic, SLOTS);
      final Outcome onV2 = ProgramRun.run(v2, contract, callee, secondCallee, GAS, isStatic, SLOTS);
      final Outcome tracedOnV1 =
          ProgramRun.runTraced(v1, contract, callee, secondCallee, GAS, isStatic, SLOTS);
      final Outcome tracedOnV2 =
          ProgramRun.runTraced(v2, contract, callee, secondCallee, GAS, isStatic, SLOTS);
      if (!onV2.equals(onV1) || !tracedOnV2.equals(tracedOnV1)) {
        differences.add(
            String.format(
                "program %d%s%n  contract %s%n  callee %s%n  second callee %s%n  v1 %s%n  v2 %s",
                i,
                isStatic ? " in a static frame" : "",
                contract.toHexString(),
                callee.toHexString(),
                secondCallee.toHexString(),
                tracedOnV1,
                tracedOnV2));
      }
    }
    assertThat(differences).isEmpty();
  }

  /** A random program of operations, each preceded by pushes of the operands it takes. */
  private static Bytes program(
      final Random random,
      final List<Integer> opcodes,
      final List<Integer> statefulOpcodes,
      final Operation[] operations) {
    final List<Bytes> parts = new ArrayList<>();
    final int length = 5 + random.nextInt(40);
    for (int step = 0; step < length; step++) {
      final List<Integer> choices = random.nextBoolean() ? statefulOpcodes : opcodes;
      final int opcode = choices.get(random.nextInt(choices.size()));
      final int operands = Math.max(0, operations[opcode].getStackItemsConsumed());
      for (int operand = 0; operand < Math.min(operands, 8); operand++) {
        final List<Bytes> values = random.nextBoolean() ? COMMON_OPERANDS : OPERANDS;
        final Bytes value = values.get(random.nextInt(values.size()));
        // PUSH1 to PUSH32, by the size of the value
        parts.add(Bytes.of(0x5f + value.size()));
        parts.add(value);
      }
      parts.add(Bytes.of(opcode));
      if (opcode >= 0x60 && opcode <= 0x7f) {
        parts.add(randomBytes(random, opcode - 0x5f));
      } else if (opcode >= 0xe6 && opcode <= 0xe8) {
        parts.add(Bytes.of(random.nextInt(256)));
      }
    }
    // JUMPDESTs at some multiples of 8, so that jumps to small operands can land
    final MutableBytes code = Bytes.concatenate(parts).mutableCopy();
    for (int i = 0; i < code.size(); i += 8) {
      if (random.nextInt(4) == 0) {
        code.set(i, (byte) 0x5b);
      }
    }
    return code;
  }

  private static Bytes randomBytes(final Random random, final int size) {
    final byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return Bytes.wrap(bytes);
  }
}
