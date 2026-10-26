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
import static org.hyperledger.besu.evm.v2.PostTxContexts.DEPLOYED_CODE_HASH;
import static org.hyperledger.besu.evm.v2.PostTxContexts.MAX_COST;
import static org.hyperledger.besu.evm.v2.PostTxContexts.T1;
import static org.hyperledger.besu.evm.v2.PostTxContexts.T2;
import static org.hyperledger.besu.evm.v2.PostTxContexts.T3;
import static org.hyperledger.besu.evm.v2.ProgramRun.CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.CONTRACT;
import static org.hyperledger.besu.evm.v2.ProgramRun.SECOND_CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The values, halts and gas of the EIP-7906 assertion operations, on the v1 interpreter and on EVM
 * v2, against {@link PostTxContexts#sampleDiff()}.
 */
class TransactionAssertionOpcodesTest {

  private static final long GAS = 100_000L;
  private static final List<Bytes32> SLOTS = List.of(Bytes32.ZERO);
  private static final Bytes TWO_TO_THE_64 =
      Bytes.concatenate(Bytes.of(1), Bytes.wrap(new byte[8]));
  private static final Bytes TWO_TO_THE_255 =
      Bytes.concatenate(Bytes.of(0x80), Bytes.wrap(new byte[31]));

  private static EVM evm(final EvmConfiguration configuration) {
    return MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, configuration);
  }

  /** PUSH32 in2, PUSH32 param, TXTRACE, STOP. */
  private static Bytes txtrace(final Bytes param, final Bytes in2) {
    return Bytes.concatenate(
        Bytes.of(0x7f),
        Bytes32.leftPad(in2),
        Bytes.of(0x7f),
        Bytes32.leftPad(param),
        Bytes.of(0xb6, 0x00));
  }

  private static ProgramRun.Outcome run(
      final EvmConfiguration configuration,
      final Bytes code,
      final FrameTransactionContext context) {
    if (context == null) {
      return ProgramRun.runTraced(
          evm(configuration), code, Bytes.EMPTY, Bytes.EMPTY, GAS, true, SLOTS);
    }
    return ProgramRun.executeTracedInFrameTransaction(
            evm(configuration), code, Bytes.EMPTY, GAS, true, SLOTS, context)
        .outcome();
  }

  private static String word(final Bytes value) {
    return Bytes32.leftPad(value).toUnprefixedHexString();
  }

  static Stream<Arguments> txtraceValues() {
    return Stream.of(
            new Object[] {0x00, 0, Bytes.of(2)},
            new Object[] {0x01, 0, Bytes.of(3)},
            new Object[] {0x02, 0, Bytes.of(1)},
            new Object[] {0x03, 0, SENDER.getBytes()},
            new Object[] {0x03, 1, CALLEE.getBytes()},
            new Object[] {0x04, 0, Bytes.ofUnsignedInt(2_000_000)},
            new Object[] {0x05, 0, Bytes.ofUnsignedInt(1_000_000)},
            new Object[] {0x04, 1, Bytes.EMPTY},
            new Object[] {0x05, 1, Bytes.of(3)},
            new Object[] {0x06, 0, CONTRACT.getBytes()},
            new Object[] {0x06, 1, CONTRACT.getBytes()},
            new Object[] {0x06, 2, CALLEE.getBytes()},
            new Object[] {0x07, 0, Bytes.of(1)},
            new Object[] {0x07, 1, Bytes.of(2)},
            new Object[] {0x07, 2, Bytes.EMPTY},
            new Object[] {0x08, 0, Bytes.EMPTY},
            new Object[] {0x08, 1, Bytes.of(5)},
            new Object[] {0x08, 2, Bytes.of(9)},
            new Object[] {0x09, 0, Bytes.of(7)},
            new Object[] {0x09, 1, Bytes.EMPTY},
            new Object[] {0x09, 2, Bytes.of(1)},
            new Object[] {0x0A, 0, SECOND_CALLEE.getBytes()},
            new Object[] {0x0B, 0, DEPLOYED_CODE_HASH.getBytes()},
            new Object[] {0x0C, 0, Bytes.of(3)},
            new Object[] {0x0D, 0, CONTRACT.getBytes()},
            new Object[] {0x0D, 1, CALLEE.getBytes()},
            new Object[] {0x0D, 2, CONTRACT.getBytes()},
            new Object[] {0x0E, 0, Bytes.of(3)},
            new Object[] {0x0E, 1, Bytes.EMPTY},
            new Object[] {0x0E, 2, Bytes.of(4)},
            new Object[] {0x0F, 0, T1},
            new Object[] {0x10, 0, T3},
            new Object[] {0x11, 0, T2},
            new Object[] {0x11, 2, T2},
            new Object[] {0x12, 2, T3},
            new Object[] {0x13, 0, Bytes.of(40)},
            new Object[] {0x13, 1, Bytes.EMPTY},
            new Object[] {0x13, 2, Bytes.of(1)},
            new Object[] {0x14, 0, MAX_COST.toBytes()},
            new Object[] {0x15, 0, SENDER.getBytes()})
        .flatMap(
            c ->
                Stream.of(ProgramRun.V1, ProgramRun.V2)
                    .map(
                        v ->
                            Arguments.of(
                                v.enableEvmV2() ? "v2" : "v1",
                                String.format(
                                    "param 0x%02x index %d", (Integer) c[0], (Integer) c[1]),
                                v,
                                c[0],
                                c[1],
                                c[2])));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("txtraceValues")
  void txtraceReadsTheDiff(
      final String evmName,
      final String name,
      final EvmConfiguration configuration,
      final int param,
      final int index,
      final Bytes expected) {
    final ProgramRun.Outcome outcome =
        run(
            configuration,
            txtrace(Bytes.of(param), Bytes.of(index)),
            PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEmpty();
    assertThat(outcome.stack()).containsExactly(word(expected));
    // two PUSH32 and a flat warm access
    assertThat(outcome.remainingGas()).isEqualTo(GAS - 3 - 3 - 100);
    // nothing is recorded in the block access list beyond what the call itself recorded
    final ProgramRun.Outcome withoutTxtrace =
        run(
            configuration,
            Bytes.concatenate(
                Bytes.of(0x7f), Bytes32.ZERO, Bytes.of(0x7f), Bytes32.ZERO, Bytes.of(0x00)),
            PostTxContexts.withDiff(true));
    assertThat(outcome.accesses()).isEqualTo(withoutTxtrace.accesses());
  }

  static Stream<Arguments> txtraceHalts() {
    return Stream.of(
            new Object[] {
              "count with a non-zero index", Bytes.of(0x00), Bytes.of(1), "INVALID_OPERATION"
            },
            new Object[] {
              "count with index 2^255", Bytes.of(0x0C), TWO_TO_THE_255, "INVALID_OPERATION"
            },
            new Object[] {
              "pre-charge with an index", Bytes.of(0x14), Bytes.of(1), "INVALID_OPERATION"
            },
            new Object[] {
              "payer with an index", Bytes.of(0x15), TWO_TO_THE_64, "INVALID_OPERATION"
            },
            new Object[] {"undefined 0x16", Bytes.of(0x16), Bytes.EMPTY, "INVALID_OPERATION"},
            new Object[] {"undefined 2^64", TWO_TO_THE_64, Bytes.EMPTY, "INVALID_OPERATION"},
            new Object[] {
              "undefined 2^64 + 1",
              Bytes.of(1, 0, 0, 0, 0, 0, 0, 0, 1),
              Bytes.EMPTY,
              "INVALID_OPERATION"
            },
            new Object[] {"undefined 2^255", TWO_TO_THE_255, Bytes.EMPTY, "INVALID_OPERATION"},
            new Object[] {"balance past the end", Bytes.of(0x03), Bytes.of(2), "OUT_OF_BOUNDS"},
            new Object[] {"balance at 2^64", Bytes.of(0x05), TWO_TO_THE_64, "OUT_OF_BOUNDS"},
            new Object[] {"slot past the end", Bytes.of(0x09), Bytes.of(3), "OUT_OF_BOUNDS"},
            new Object[] {"deployed past the end", Bytes.of(0x0B), Bytes.of(1), "OUT_OF_BOUNDS"},
            new Object[] {"event past the end", Bytes.of(0x0D), Bytes.of(3), "OUT_OF_BOUNDS"},
            new Object[] {"topic of a LOG0", Bytes.of(0x0F), Bytes.of(1), "OUT_OF_BOUNDS"},
            new Object[] {"fourth topic of a LOG3", Bytes.of(0x12), Bytes.EMPTY, "OUT_OF_BOUNDS"})
        .flatMap(
            c ->
                Stream.of(ProgramRun.V1, ProgramRun.V2)
                    .map(
                        v ->
                            Arguments.of(
                                v.enableEvmV2() ? "v2" : "v1", c[0], v, c[1], c[2], c[3])));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("txtraceHalts")
  void txtraceHaltsExceptionally(
      final String evmName,
      final String name,
      final EvmConfiguration configuration,
      final Bytes param,
      final Bytes index,
      final String halt) {
    final ProgramRun.Outcome outcome =
        run(configuration, txtrace(param, index), PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEqualTo(halt);
    assertThat(outcome.remainingGas()).isZero();
  }

  static Stream<Arguments> evms() {
    return Stream.of(Arguments.of("v1", ProgramRun.V1), Arguments.of("v2", ProgramRun.V2));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txtraceHaltsOutsideAPostTxFrame(final String evmName, final EvmConfiguration configuration) {
    final FrameTransactionContext senderFrame = PostTxContexts.withDiff(true);
    senderFrame.setCurrentFrameIndex(1);
    final Bytes code = txtrace(Bytes.of(0x00), Bytes.EMPTY);

    assertThat(run(configuration, code, senderFrame).haltReason()).isEqualTo("INVALID_OPERATION");
    // not a frame transaction: the halt consumes everything
    final ProgramRun.Outcome legacy = run(configuration, code, null);
    assertThat(legacy.haltReason()).isEqualTo("INVALID_OPERATION");
    assertThat(legacy.remainingGas()).isZero();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txtraceReadsAnEmptyDiffAndThePayerContext(
      final String evmName, final EvmConfiguration configuration) {
    final FrameTransactionContext empty = PostTxContexts.withEmptyDiff();
    assertThat(run(configuration, txtrace(Bytes.of(0x00), Bytes.EMPTY), empty).stack())
        .containsExactly(word(Bytes.EMPTY));
    assertThat(
            run(configuration, txtrace(Bytes.of(0x03), Bytes.EMPTY), PostTxContexts.withEmptyDiff())
                .haltReason())
        .isEqualTo("OUT_OF_BOUNDS");
    // without a payer, GAS_PAYER reads 0
    assertThat(
            run(configuration, txtrace(Bytes.of(0x15), Bytes.EMPTY), PostTxContexts.withDiff(false))
                .stack())
        .containsExactly(word(Bytes.EMPTY));
    // with nothing installed, the pre-charge still reads and the tables halt
    assertThat(
            run(
                    configuration,
                    txtrace(Bytes.of(0x14), Bytes.EMPTY),
                    PostTxContexts.withoutDiff(true))
                .stack())
        .containsExactly(word(MAX_COST.toBytes()));
    assertThat(
            run(
                    configuration,
                    txtrace(Bytes.of(0x0C), Bytes.EMPTY),
                    PostTxContexts.withoutDiff(true))
                .haltReason())
        .isEqualTo("INVALID_OPERATION");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txtraceChecksItsGasBeforeReading(
      final String evmName, final EvmConfiguration configuration) {
    final Bytes code = txtrace(Bytes.of(0x00), Bytes.EMPTY);
    final ProgramRun.Outcome exact =
        ProgramRun.executeTracedInFrameTransaction(
                evm(configuration),
                code,
                Bytes.EMPTY,
                106L,
                true,
                SLOTS,
                PostTxContexts.withDiff(true))
            .outcome();
    assertThat(exact.haltReason()).isEmpty();
    assertThat(exact.remainingGas()).isZero();
    final ProgramRun.Outcome oneShort =
        ProgramRun.executeTracedInFrameTransaction(
                evm(configuration),
                code,
                Bytes.EMPTY,
                105L,
                true,
                SLOTS,
                PostTxContexts.withDiff(true))
            .outcome();
    assertThat(oneShort.haltReason()).isEqualTo("INSUFFICIENT_GAS");
  }

  /** PUSH32 length, PUSH32 dataOffset, PUSH32 memOffset, PUSH32 eventIndex, EVENTDATACOPY, STOP. */
  private static Bytes eventDataCopy(
      final Bytes eventIndex, final Bytes memOffset, final Bytes dataOffset, final Bytes length) {
    return Bytes.concatenate(
        Bytes.of(0x7f),
        Bytes32.leftPad(length),
        Bytes.of(0x7f),
        Bytes32.leftPad(dataOffset),
        Bytes.of(0x7f),
        Bytes32.leftPad(memOffset),
        Bytes.of(0x7f),
        Bytes32.leftPad(eventIndex),
        Bytes.of(0xb8, 0x00));
  }

  static Stream<Arguments> eventDataCopies() {
    final Bytes data = PostTxContexts.EVENT_DATA;
    return Stream.of(
            // name, event, memory offset, data offset, length, expected memory, copy gas
            new Object[] {
              "whole data",
              0,
              0,
              0,
              40,
              Bytes.concatenate(data, Bytes.wrap(new byte[24])),
              3 + 6 + 6
            },
            new Object[] {
              "across a word boundary",
              0,
              1,
              0,
              40,
              Bytes.concatenate(Bytes.of(0), data, Bytes.wrap(new byte[23])),
              3 + 6 + 6
            },
            new Object[] {"from an offset", 0, 0, 8, 32, data.slice(8, 32), 3 + 3 + 3},
            new Object[] {"zero length at the end", 0, 0, 40, 0, Bytes.EMPTY, 3},
            new Object[] {"zero length of empty data", 1, 0, 0, 0, Bytes.EMPTY, 3},
            new Object[] {
              "last event",
              2,
              0,
              0,
              1,
              Bytes.concatenate(Bytes.of(0x42), Bytes.wrap(new byte[31])),
              3 + 3 + 3
            })
        .flatMap(
            c ->
                Stream.of(ProgramRun.V1, ProgramRun.V2)
                    .map(
                        v ->
                            Arguments.of(
                                v.enableEvmV2() ? "v2" : "v1",
                                c[0],
                                v,
                                c[1],
                                c[2],
                                c[3],
                                c[4],
                                c[5],
                                c[6])));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("eventDataCopies")
  void eventDataCopyCopiesTheEventData(
      final String evmName,
      final String name,
      final EvmConfiguration configuration,
      final int event,
      final int memOffset,
      final int dataOffset,
      final int length,
      final Bytes expectedMemory,
      final int copyGas) {
    final ProgramRun.Outcome outcome =
        run(
            configuration,
            eventDataCopy(
                Bytes.of(event), Bytes.of(memOffset), Bytes.of(dataOffset), Bytes.of(length)),
            PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEmpty();
    assertThat(outcome.memory()).isEqualTo(expectedMemory.toHexString());
    // four PUSH32, then 3, 3 per word copied and the memory expansion
    assertThat(outcome.remainingGas()).isEqualTo(GAS - 4 * 3 - copyGas);
  }

  static Stream<Arguments> eventDataCopyHalts() {
    final Bytes allOnes = Bytes32.ZERO.not();
    return Stream.of(
            new Object[] {"event past the end", Bytes.of(3), Bytes.EMPTY, Bytes.EMPTY},
            new Object[] {"event 2^64", TWO_TO_THE_64, Bytes.EMPTY, Bytes.EMPTY},
            new Object[] {"range past the end", Bytes.EMPTY, Bytes.of(32), Bytes.of(9)},
            new Object[] {"offset at the end", Bytes.EMPTY, Bytes.of(40), Bytes.of(1)},
            new Object[] {"zero length past the end", Bytes.EMPTY, Bytes.of(41), Bytes.EMPTY},
            new Object[] {"one byte of empty data", Bytes.of(1), Bytes.EMPTY, Bytes.of(1)},
            new Object[] {"offset 2^64-1", Bytes.EMPTY, Bytes.repeat((byte) 0xff, 8), Bytes.of(1)},
            new Object[] {"offset 2^256-1, which would wrap", Bytes.EMPTY, allOnes, Bytes.of(1)})
        .flatMap(
            c ->
                Stream.of(ProgramRun.V1, ProgramRun.V2)
                    .map(
                        v ->
                            Arguments.of(
                                v.enableEvmV2() ? "v2" : "v1", c[0], v, c[1], c[2], c[3])));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("eventDataCopyHalts")
  void eventDataCopyHaltsOnARangePastTheData(
      final String evmName,
      final String name,
      final EvmConfiguration configuration,
      final Bytes event,
      final Bytes dataOffset,
      final Bytes length) {
    final ProgramRun.Outcome outcome =
        run(
            configuration,
            eventDataCopy(event, Bytes.EMPTY, dataOffset, length),
            PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEqualTo("OUT_OF_BOUNDS");
    assertThat(outcome.remainingGas()).isZero();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void eventDataCopyOfNothingAtAHugeMemoryOffsetExpandsNothing(
      final String evmName, final EvmConfiguration configuration) {
    final ProgramRun.Outcome outcome =
        run(
            configuration,
            eventDataCopy(Bytes.EMPTY, Bytes32.ZERO.not(), Bytes.EMPTY, Bytes.EMPTY),
            PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEmpty();
    assertThat(outcome.memory()).isEqualTo("0x");
    assertThat(outcome.remainingGas()).isEqualTo(GAS - 4 * 3 - 3);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void eventDataCopyHaltsOutsideAPostTxFrame(
      final String evmName, final EvmConfiguration configuration) {
    final FrameTransactionContext senderFrame = PostTxContexts.withDiff(true);
    senderFrame.setCurrentFrameIndex(1);
    final Bytes code = eventDataCopy(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY, Bytes.of(1));

    assertThat(run(configuration, code, senderFrame).haltReason()).isEqualTo("INVALID_OPERATION");
    assertThat(run(configuration, code, null).haltReason()).isEqualTo("INVALID_OPERATION");
    assertThat(run(configuration, code, PostTxContexts.withoutDiff(true)).haltReason())
        .isEqualTo("INVALID_OPERATION");
  }
}
