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
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.v2.ProgramRun.Fork;
import org.hyperledger.besu.evm.v2.ProgramRun.Outcome;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/**
 * Runs every opcode on the v1 interpreter and on EVM v2, for each fork, and requires both to end in
 * the same state. EVM v2 takes the opcodes a fork enables, and their configuration, from the fork's
 * operation registry, and this keeps the two interpreters in line with each other.
 */
class EveryOpcodeV1V2DifferentialTest {

  /**
   * Returns the call value, the caller and its own address as 96 bytes, and stores 1 in slot 0 when
   * it is called without value, so that a call shows what context it ran in, and whether it was
   * static.
   */
  private static final Bytes CALLEE_CODE =
      Bytes.fromHexString(
          "0x"
              + "34600052" // CALLVALUE PUSH1 0 MSTORE
              + "33602052" // CALLER PUSH1 0x20 MSTORE
              + "30604052" // ADDRESS PUSH1 0x40 MSTORE
              + "34601557" // CALLVALUE PUSH1 0x15 JUMPI
              + "6001600055" // PUSH1 1 PUSH1 0 SSTORE
              + "5b" // JUMPDEST
              + "60606000f3"); // PUSH1 0x60 PUSH1 0 RETURN

  private static final Bytes32 PATTERN =
      Bytes32.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");

  static final List<Fork> FORKS = ProgramRun.FORKS;

  /**
   * The stacks each opcode runs on, top first: none at all, small numbers, which most operations
   * take as offsets, sizes and indices, calls to the callee with and without value, one with more
   * value than the contract has, values at the top of the word range, signed values on both sides
   * of zero, the accounts of the test world as the addresses PAY, BALANCE, the EXTCODE operations
   * and SELFDESTRUCT read, and a mix of values whose limbs all differ.
   */
  private static final Map<String, List<Bytes>> STACKS = new LinkedHashMap<>();

  static {
    STACKS.put("empty", List.of());
    STACKS.put("one item", List.of(Bytes.of(1)));
    STACKS.put("small numbers", IntStream.rangeClosed(1, 18).mapToObj(Bytes::of).toList());
    STACKS.put("call", call(Bytes.of(0)));
    STACKS.put("call with value", call(Bytes.of(1)));
    STACKS.put("call with more value than the contract has", call(Bytes.of(0x1e, 0x84, 0x80)));
    STACKS.put(
        "large values",
        List.of(
            Bytes32.ZERO.not(),
            Bytes.concatenate(Bytes.of(0x80), Bytes.wrap(new byte[31])),
            Bytes32.ZERO.not(),
            Bytes.of(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
            Bytes.of(1),
            Bytes.of(0),
            Bytes.of(0)));
    STACKS.put(
        "signed values",
        List.of(
            Bytes.of(1),
            Bytes.concatenate(Bytes.of(0x80), Bytes.wrap(new byte[31])),
            Bytes32.ZERO.not(),
            Bytes.concatenate(Bytes.of(0x7f), Bytes32.ZERO.not().slice(1)),
            Bytes.of(0),
            Bytes.of(0x80)));
    STACKS.put(
        "accounts",
        List.of(
            EMPTY_ACCOUNT.getBytes(),
            CALLEE.getBytes(),
            CONTRACT.getBytes(),
            SECOND_CALLEE.getBytes(),
            SENDER.getBytes()));
    STACKS.put(
        "mixed values",
        List.of(
            Bytes.of(0),
            PATTERN,
            Bytes.of(0x1f),
            Bytes.concatenate(Bytes.of(0x80), Bytes.wrap(new byte[31])),
            Bytes.of(7),
            Bytes32.ZERO.not(),
            Bytes.of(0x21),
            CALLEE.getBytes()));
  }

  /** The storage and transient storage slots the stacks above can write. */
  private static final List<Bytes32> SLOTS =
      IntStream.rangeClosed(0, 17).mapToObj(i -> Bytes32.leftPad(Bytes.of(i))).toList();

  /**
   * The gas left for an opcode that halts: none, and one less than the base, very low, low, mid,
   * high, BLOCKHASH, warm and cold access costs, and more than any of them.
   */
  private static final List<Long> GAS_FOR_THE_OPCODE =
      List.of(0L, 1L, 2L, 4L, 7L, 9L, 19L, 99L, 2_599L, 30_000L);

  @ParameterizedTest(name = "{0}")
  @FieldSource("FORKS")
  void everyOpcodeEndsInTheSameStateOnV1AndV2(final Fork fork) {
    final EVM v1 = fork.factory().apply(ProgramRun.V1);
    final EVM v2 = fork.factory().apply(ProgramRun.V2);
    final Bytes prelude = prelude(v1);
    final List<String> differences = new ArrayList<>();
    for (int opcode = 0; opcode < 0x100; opcode++) {
      final boolean isDefined = !(v1.getOperationsUnsafe()[opcode] instanceof InvalidOperation);
      for (final Map.Entry<String, List<Bytes>> stack : STACKS.entrySet()) {
        for (final boolean isStatic : List.of(false, true)) {
          // the prelude writes state, which a static frame cannot
          for (final Bytes start :
              isStatic ? List.of(Bytes.EMPTY) : List.of(Bytes.EMPTY, prelude)) {
            // 0x00 as the immediate makes the opcode the last operation that costs gas; 0x80
            // decodes
            // to the small stack indices DUPN, SWAPN and EXCHANGE can reach on these stacks
            for (final int immediate : List.of(0x00, 0x80)) {
              final Bytes code =
                  Bytes.concatenate(
                      start, push(stack.getValue()), Bytes.of(opcode, immediate, 0x00));
              final String program =
                  String.format(
                      "opcode 0x%02x with immediate 0x%02x on the %s stack%s%s",
                      opcode,
                      immediate,
                      stack.getKey(),
                      start.isEmpty() ? "" : " after the prelude",
                      isStatic ? " in a static frame" : "");
              compare(v1, v2, code, isStatic, isDefined, program, differences);
            }
          }
        }
      }
    }
    assertThat(differences).isEmpty();
  }

  private static void compare(
      final EVM v1,
      final EVM v2,
      final Bytes code,
      final boolean isStatic,
      final boolean isDefined,
      final String program,
      final List<String> differences) {
    final Outcome withPlentyOfGas = run(v1, code, 1_000_000L, isStatic);
    compare(v2, code, 1_000_000L, isStatic, withPlentyOfGas, program, differences);
    // untraced, as blocks are imported; the other runs are traced, to compare what tracers see
    final Outcome untracedOnV1 =
        ProgramRun.run(v1, code, CALLEE_CODE, Bytes.EMPTY, 1_000_000L, isStatic, SLOTS);
    final Outcome untracedOnV2 =
        ProgramRun.run(v2, code, CALLEE_CODE, Bytes.EMPTY, 1_000_000L, isStatic, SLOTS);
    if (!untracedOnV2.equals(untracedOnV1)) {
      differences.add(
          String.format("%s untraced:%n  v1 %s%n  v2 %s", program, untracedOnV1, untracedOnV2));
    }
    if (!isDefined) {
      // an opcode the fork does not define halts whatever the gas
      return;
    }
    final List<Long> gasLimits;
    if (!withPlentyOfGas.haltReason().isEmpty()) {
      // a halt consumes all the gas, so try the opcode with too little gas for it and with
      // enough, as whether it halts for gas or for the stack depends on which it checks first
      final long beforeOpcode =
          1_000_000L - run(v1, code.slice(0, code.size() - 3), 1_000_000L, isStatic).remainingGas();
      gasLimits = GAS_FOR_THE_OPCODE.stream().map(gas -> beforeOpcode + gas).toList();
    } else {
      // with exactly the gas the program needs, and with one less
      final long used = 1_000_000L - withPlentyOfGas.remainingGas();
      gasLimits = List.of(used, Math.max(0, used - 1));
    }
    for (final long gas : gasLimits) {
      compare(v2, code, gas, isStatic, run(v1, code, gas, isStatic), program, differences);
    }
  }

  private static void compare(
      final EVM v2,
      final Bytes code,
      final long gas,
      final boolean isStatic,
      final Outcome onV1,
      final String program,
      final List<String> differences) {
    final Outcome onV2 = run(v2, code, gas, isStatic);
    if (!onV2.equals(onV1)) {
      differences.add(String.format("%s with %d gas:%n  v1 %s%n  v2 %s", program, gas, onV1, onV2));
    }
  }

  private static Outcome run(
      final EVM evm, final Bytes code, final long gas, final boolean isStatic) {
    return ProgramRun.runTraced(evm, code, CALLEE_CODE, Bytes.EMPTY, gas, isStatic, SLOTS);
  }

  /**
   * Leaves memory, storage, transient storage, return data and the warm callee behind, for the
   * opcodes that read them. Uses only the opcodes the fork has.
   */
  private static Bytes prelude(final EVM evm) {
    final List<Bytes> parts = new ArrayList<>();
    // PUSH32 PATTERN PUSH1 0 MSTORE
    parts.add(Bytes.of(0x7f));
    parts.add(PATTERN);
    parts.add(Bytes.fromHexString("0x600052"));
    // PUSH1 0x2a PUSH1 1 SSTORE
    parts.add(Bytes.fromHexString("0x602a600155"));
    if (!(evm.getOperationsUnsafe()[0x5d] instanceof InvalidOperation)) {
      // PUSH1 0x2b PUSH1 1 TSTORE
      parts.add(Bytes.fromHexString("0x602b60015d"));
    }
    // CALL the callee without value or input, keeping only its return data
    parts.add(Bytes.fromHexString("0x6000600060006000600073"));
    parts.add(CALLEE.getBytes());
    parts.add(Bytes.fromHexString("0x620fffff" + "f1" + "50"));
    return Bytes.concatenate(parts);
  }

  /** The arguments of a CALL to the callee with the value, top first. */
  private static List<Bytes> call(final Bytes value) {
    return List.of(
        Bytes.of(0xff, 0xff),
        CALLEE.getBytes(),
        value,
        Bytes.of(0),
        Bytes.of(0x20),
        Bytes.of(0),
        Bytes.of(0x20),
        Bytes.of(0));
  }

  /** Pushes the items, given top first, with one PUSH32 each. */
  private static Bytes push(final List<Bytes> topFirst) {
    final List<Bytes> pushes = new ArrayList<>();
    for (int i = topFirst.size() - 1; i >= 0; i--) {
      pushes.add(Bytes.of(0x7f));
      pushes.add(Bytes32.leftPad(topFirst.get(i)));
    }
    return Bytes.concatenate(pushes);
  }
}
