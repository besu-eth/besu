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
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.FrameInfo;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.SignatureInfo;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.v2.ProgramRun.Execution;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Runs the EIP-8141 opcodes on the v1 interpreter and on EVM v2 inside a frame transaction, and
 * requires both to end in the same state, including the approval context.
 */
class FrameOpcodesV1V2DifferentialTest {

  private static final List<Integer> OPCODES = List.of(0xaa, 0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5);

  private static final List<Bytes32> SLOTS =
      IntStream.rangeClosed(0, 3).mapToObj(i -> Bytes32.leftPad(Bytes.of(i))).toList();

  private static final Bytes32 PATTERN =
      Bytes32.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");

  private static final List<Long> GAS = List.of(0L, 1L, 2L, 3L, 5L, 50L, 1_000_000L);

  /** Contexts that select every branch of the operations: who may approve, and what has run. */
  private static final Map<String, Supplier<FrameTransactionContext>> CONTEXTS =
      new LinkedHashMap<>();

  static {
    CONTEXTS.put("first frame, legacy nonce", () -> context(SENDER, List.of(UInt256.ZERO), 0));
    CONTEXTS.put("contract is the sender", () -> context(CONTRACT, List.of(UInt256.ZERO), 0));
    CONTEXTS.put(
        "contract is the sender, keyed nonces",
        () -> context(CONTRACT, List.of(UInt256.valueOf(5), UInt256.valueOf(7)), 0));
    CONTEXTS.put("second frame", () -> context(SENDER, List.of(UInt256.ZERO), 1));
    // EIP-7906: APPROVE halts in a POST_TX frame, which FRAMEPARAM reports as mode 3
    CONTEXTS.put("post-tx frame", () -> PostTxContexts.withoutDiff(true));
    CONTEXTS.put("post-tx frame, no payer", () -> PostTxContexts.withoutDiff(false));
  }

  /** The stacks each opcode runs on, top first. */
  private static final Map<String, List<Bytes>> STACKS = new LinkedHashMap<>();

  static {
    STACKS.put("empty", List.of());
    STACKS.put("one item", List.of(Bytes.of(1)));
    for (int param = 0; param <= 0x11; param++) {
      for (int index = 0; index <= 2; index++) {
        // TXPARAM reads the top; FRAMEPARAM and SIGPARAM an index above the parameter
        STACKS.put(
            String.format("index %d, parameter 0x%02x", index, param),
            List.of(Bytes.of(index), Bytes.of(param)));
      }
      STACKS.put(String.format("parameter 0x%02x", param), List.of(Bytes.of(param)));
    }
    for (int scope = 0; scope <= 4; scope++) {
      // APPROVE takes offset, length, scope; the copies take memory offset, source offset,
      // length and index
      STACKS.put("approve scope " + scope, List.of(Bytes.of(0), Bytes.of(0x20), Bytes.of(scope)));
    }
    for (int index = 0; index <= 2; index++) {
      STACKS.put(
          "copy from index " + index,
          List.of(Bytes.of(1), Bytes.of(2), Bytes.of(0x30), Bytes.of(index)));
      STACKS.put("load past the end of index " + index, List.of(Bytes.of(0x1f), Bytes.of(index)));
    }
    STACKS.put(
        "large values",
        List.of(
            Bytes32.ZERO.not(),
            Bytes.of(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
            Bytes.of(0x01, 0x00),
            Bytes.concatenate(Bytes.of(0x80), Bytes.wrap(new byte[31]))));
    STACKS.put(
        "an index just past an int",
        List.of(Bytes.of(0x80, 0, 0, 0), Bytes.of(0x80, 0, 0, 0), Bytes.of(0), Bytes.of(0)));
  }

  @Test
  void frameOpcodesEndInTheSameStateOnV1AndV2() {
    final EVM v1 = MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, ProgramRun.V1);
    final EVM v2 = MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, ProgramRun.V2);
    // leaves a pattern in memory, for APPROVE to return
    final Bytes prelude =
        Bytes.concatenate(Bytes.of(0x7f), PATTERN, Bytes.fromHexString("0x600052"));
    final List<String> differences = new ArrayList<>();
    int runs = 0;
    for (final int opcode : OPCODES) {
      for (final Map.Entry<String, List<Bytes>> stack : STACKS.entrySet()) {
        for (final Map.Entry<String, Supplier<FrameTransactionContext>> context :
            CONTEXTS.entrySet()) {
          for (final boolean isStatic : List.of(false, true)) {
            final Bytes code =
                Bytes.concatenate(prelude, push(stack.getValue()), Bytes.of(opcode, 0x00));
            final long beforeOpcode =
                1_000_000L
                    - run(
                            v1,
                            Bytes.concatenate(prelude, push(stack.getValue())),
                            1_000_000L,
                            isStatic,
                            context)
                        .frame()
                        .getRemainingGas();
            for (final long gas : GAS) {
              final long limit = Math.min(1_000_000L, beforeOpcode + gas);
              final String onV1 = describe(run(v1, code, limit, isStatic, context));
              final String onV2 = describe(run(v2, code, limit, isStatic, context));
              runs++;
              if (!onV2.equals(onV1)) {
                differences.add(
                    String.format(
                        "opcode 0x%02x on the %s stack, %s%s, with %d gas:%n  v1 %s%n  v2 %s",
                        opcode,
                        stack.getKey(),
                        context.getKey(),
                        isStatic ? ", static" : "",
                        limit,
                        onV1,
                        onV2));
              }
            }
          }
        }
      }
    }
    assertThat(runs).isGreaterThan(10_000);
    assertThat(differences).isEmpty();
  }

  /**
   * EIP-7906: APPROVE halts exceptionally anywhere in a POST_TX frame, before the checks that would
   * revert it: the scope beyond the mask and, in a nested call, the target mismatch.
   */
  @Test
  void approveHaltsInAPostTxFrameOnV1AndV2() {
    final EVM v1 = MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, ProgramRun.V1);
    final EVM v2 = MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, ProgramRun.V2);
    for (final int scope : List.of(1, 2, 3, 4)) {
      // APPROVE(offset 0, length 0, scope)
      final Bytes approve = Bytes.of(0x60, scope, 0x60, 0x00, 0x60, 0x00, 0xaa);
      for (final EVM evm : List.of(v1, v2)) {
        final Execution direct =
            ProgramRun.executeTracedInFrameTransaction(
                evm, approve, Bytes.EMPTY, 100_000L, true, SLOTS, PostTxContexts.withoutDiff(true));
        assertThat(direct.outcome().haltReason()).isEqualTo("ILLEGAL_STATE_CHANGE");
        assertThat(direct.outcome().remainingGas()).isZero();

        // CALL(100000, CALLEE, 0, 0, 0, 0, 0), where the callee runs APPROVE
        final Bytes call =
            Bytes.fromHexString("0x600060006000600060006" + "2ca11ee" + "620186a0f100");
        final Execution nested =
            ProgramRun.executeTracedInFrameTransaction(
                evm, call, approve, 200_000L, true, SLOTS, PostTxContexts.withoutDiff(true));
        assertThat(nested.outcome().haltReason()).isEmpty();
        assertThat(nested.outcome().stack()).containsExactly("00".repeat(32));
        assertThat(nested.outcome().steps())
            .anyMatch(step -> step.contains("APPROVE") && step.contains("ILLEGAL_STATE_CHANGE"));
        // the callee consumed all of the 100000 it was given
        assertThat(nested.outcome().remainingGas()).isLessThan(100_000L);
      }
      assertThat(
              describe(
                  ProgramRun.executeTracedInFrameTransaction(
                      v1,
                      approve,
                      Bytes.EMPTY,
                      100_000L,
                      true,
                      SLOTS,
                      PostTxContexts.withoutDiff(true))))
          .isEqualTo(
              describe(
                  ProgramRun.executeTracedInFrameTransaction(
                      v2,
                      approve,
                      Bytes.EMPTY,
                      100_000L,
                      true,
                      SLOTS,
                      PostTxContexts.withoutDiff(true))));
    }
  }

  private static Execution run(
      final EVM evm,
      final Bytes code,
      final long gas,
      final boolean isStatic,
      final Map.Entry<String, Supplier<FrameTransactionContext>> context) {
    return ProgramRun.executeTracedInFrameTransaction(
        evm, code, Bytes.EMPTY, gas, isStatic, SLOTS, context.getValue().get());
  }

  private static String describe(final Execution execution) {
    final FrameTransactionContext context = execution.frame().getFrameTransactionContext().get();
    return execution.outcome()
        + " payer "
        + context.payer()
        + " sender approved "
        + context.senderApproved();
  }

  /**
   * A context with a VERIFY frame targeting the contract, which may approve both scopes, followed
   * by a SENDER frame to the callee, and one entry of each signature kind.
   */
  private static FrameTransactionContext context(
      final Address sender, final List<UInt256> nonceKeys, final int currentFrame) {
    final FrameTransactionContext context =
        new FrameTransactionContext(
            List.of(
                new FrameInfo(
                    FrameTransactionContext.MODE_VERIFY,
                    FrameTransactionContext.APPROVE_SCOPE_MASK,
                    CONTRACT,
                    50_000L,
                    10_000L,
                    Wei.ZERO,
                    Bytes.concatenate(PATTERN, Bytes.of(0xab, 0xcd))),
                new FrameInfo(
                    FrameTransactionContext.MODE_SENDER,
                    FrameTransactionContext.ATOMIC_BATCH_FLAG,
                    CALLEE,
                    70_000L,
                    0L,
                    Wei.of(3),
                    Bytes.of(0x11, 0x22, 0x33))),
            List.of(
                new SignatureInfo(
                    FrameTransactionContext.SCHEME_SECP256K1,
                    sender,
                    Bytes32.leftPad(Bytes.of(0x5e)),
                    null),
                new SignatureInfo(
                    FrameTransactionContext.SCHEME_ARBITRARY,
                    null,
                    null,
                    Bytes.fromHexString("0xa1a2a3a4a5"))),
            sender,
            PATTERN,
            9L,
            nonceKeys,
            4L,
            Wei.of(2),
            Wei.of(11),
            Wei.of(13),
            Wei.of(1_000));
    if (currentFrame > 0) {
      context.recordFrameCompletion(0, FrameTransactionContext.STATUS_SUCCESS, 21_000L, 300L);
    }
    context.setCurrentFrameIndex(currentFrame);
    return context;
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
