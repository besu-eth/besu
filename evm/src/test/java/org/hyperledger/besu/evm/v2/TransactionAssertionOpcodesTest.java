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
import static org.hyperledger.besu.evm.v2.PostTxContexts.DELEGATED;
import static org.hyperledger.besu.evm.v2.PostTxContexts.DEPLOYED_CODE_HASH;
import static org.hyperledger.besu.evm.v2.PostTxContexts.MAX_COST;
import static org.hyperledger.besu.evm.v2.PostTxContexts.NEVER_EXISTED;
import static org.hyperledger.besu.evm.v2.PostTxContexts.T1;
import static org.hyperledger.besu.evm.v2.PostTxContexts.T2;
import static org.hyperledger.besu.evm.v2.PostTxContexts.T3;
import static org.hyperledger.besu.evm.v2.ProgramRun.CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.CONTRACT;
import static org.hyperledger.besu.evm.v2.ProgramRun.SECOND_CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
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

  // TXDIFF

  private static final long WARM = 100L;
  private static final long COLD_SLOT = 2_100L;
  private static final long COLD_ACCOUNT = 3_000L;
  private static final Address PRECOMPILE = Address.fromHexString("0x01");

  /** The callee's address with the twelve high bytes of the word set, which TXDIFF ignores. */
  private static final Bytes CALLEE_WITH_HIGH_BITS =
      Bytes.concatenate(Bytes.repeat((byte) 0xff, 12), CALLEE.getBytes());

  /** PUSH32 in3, PUSH32 in2, PUSH32 param, TXDIFF: 9 gas before the operation. */
  private static Bytes txdiffCall(final Bytes param, final Bytes in2, final Bytes in3) {
    return Bytes.concatenate(
        Bytes.of(0x7f),
        Bytes32.leftPad(in3),
        Bytes.of(0x7f),
        Bytes32.leftPad(in2),
        Bytes.of(0x7f),
        Bytes32.leftPad(param),
        Bytes.of(0xb7));
  }

  /** {@link #txdiffCall}, then STOP. */
  private static Bytes txdiff(final Bytes param, final Bytes in2, final Bytes in3) {
    return Bytes.concatenate(txdiffCall(param, in2, in3), Bytes.of(0x00));
  }

  private static final Pattern TXDIFF_STEP = Pattern.compile(" TXDIFF gas .* -> cost (\\d+) halt ");

  /** The gas each TXDIFF of a traced run reported, in execution order. */
  private static List<Long> txdiffCosts(final ProgramRun.Outcome outcome) {
    return outcome.steps().stream()
        .map(TXDIFF_STEP::matcher)
        .filter(Matcher::find)
        .map(m -> Long.parseLong(m.group(1)))
        .toList();
  }

  /** What a run recorded in the block access list beyond what the call itself recorded. */
  private static Set<String> recordedAccesses(
      final EvmConfiguration configuration, final ProgramRun.Outcome outcome) {
    final Set<String> accesses = new TreeSet<>(outcome.accesses());
    accesses.removeAll(
        run(configuration, Bytes.of(0x00), PostTxContexts.withDiff(true)).accesses());
    return accesses;
  }

  private static String slotAccess(final Address address, final long key) {
    return address.toHexString() + " " + UInt256.valueOf(key).toHexString();
  }

  static Stream<Arguments> txdiffValues() {
    final Bytes designatorHash =
        Hash.hash(Bytes.concatenate(Bytes.fromHexString("0xef0100"), CALLEE.getBytes())).getBytes();
    final Bytes emptyCodeHash = Hash.EMPTY.getBytes();
    return Stream.of(
            // name, param, in2, in3, value, cost; the prestate differs from the live world
            new Object[] {
              "slot before", 0x00, CONTRACT.getBytes(), Bytes.of(2), Bytes.of(5), COLD_SLOT
            },
            new Object[] {
              "slot after", 0x01, CONTRACT.getBytes(), Bytes.of(2), Bytes.EMPTY, COLD_SLOT
            },
            new Object[] {
              "slot before, address high bits ignored",
              0x00,
              CALLEE_WITH_HIGH_BITS,
              Bytes.EMPTY,
              Bytes.of(9),
              COLD_SLOT
            },
            new Object[] {
              "slot after, address high bits ignored",
              0x01,
              CALLEE_WITH_HIGH_BITS,
              Bytes.EMPTY,
              Bytes.EMPTY,
              COLD_SLOT
            },
            new Object[] {
              "slot before of a never-existing account",
              0x00,
              NEVER_EXISTED.getBytes(),
              Bytes.of(7),
              Bytes.EMPTY,
              COLD_SLOT
            },
            new Object[] {
              "slot after of a never-existing account",
              0x01,
              NEVER_EXISTED.getBytes(),
              Bytes.of(7),
              Bytes.EMPTY,
              COLD_SLOT
            },
            new Object[] {
              "slot before at key 2^255",
              0x00,
              CONTRACT.getBytes(),
              TWO_TO_THE_255,
              Bytes.EMPTY,
              COLD_SLOT
            },
            new Object[] {
              "balance before",
              0x02,
              SENDER.getBytes(),
              Bytes.EMPTY,
              Bytes.ofUnsignedInt(2_000_000),
              COLD_ACCOUNT
            },
            // the call moved its 5 wei value from the sender to the contract
            new Object[] {
              "balance after",
              0x03,
              SENDER.getBytes(),
              Bytes.EMPTY,
              Bytes.ofUnsignedInt(1_000_000 - 5),
              COLD_ACCOUNT
            },
            new Object[] {
              "balance before of a warm account",
              0x02,
              CONTRACT.getBytes(),
              Bytes.EMPTY,
              Bytes.ofUnsignedInt(1_000_000),
              WARM
            },
            new Object[] {
              "balance after of a warm account",
              0x03,
              CONTRACT.getBytes(),
              Bytes.EMPTY,
              Bytes.ofUnsignedInt(1_000_005),
              WARM
            },
            new Object[] {
              "balance after of a precompile",
              0x03,
              PRECOMPILE.getBytes(),
              Bytes.EMPTY,
              Bytes.EMPTY,
              WARM
            },
            new Object[] {
              "balance before of a never-existing account",
              0x02,
              NEVER_EXISTED.getBytes(),
              Bytes.EMPTY,
              Bytes.EMPTY,
              COLD_ACCOUNT
            },
            new Object[] {
              "balance before of an account gone since",
              0x02,
              DELEGATED.getBytes(),
              Bytes.EMPTY,
              Bytes.of(1),
              COLD_ACCOUNT
            },
            new Object[] {
              "balance after of an account gone since",
              0x03,
              DELEGATED.getBytes(),
              Bytes.EMPTY,
              Bytes.EMPTY,
              COLD_ACCOUNT
            },
            new Object[] {
              "code hash before of a delegated account, the designator's own",
              0x04,
              DELEGATED.getBytes(),
              Bytes.EMPTY,
              designatorHash,
              COLD_ACCOUNT
            },
            new Object[] {
              "code hash after of a missing account",
              0x05,
              DELEGATED.getBytes(),
              Bytes.EMPTY,
              emptyCodeHash,
              COLD_ACCOUNT
            },
            new Object[] {
              "code hash before of a never-existing account",
              0x04,
              NEVER_EXISTED.getBytes(),
              Bytes.EMPTY,
              emptyCodeHash,
              COLD_ACCOUNT
            },
            new Object[] {
              "code hash after of an account without code",
              0x05,
              SENDER.getBytes(),
              Bytes.EMPTY,
              emptyCodeHash,
              COLD_ACCOUNT
            },
            new Object[] {
              "code hash before of a precompile",
              0x04,
              PRECOMPILE.getBytes(),
              Bytes.EMPTY,
              emptyCodeHash,
              WARM
            },
            new Object[] {
              "slots of an address", 0x06, CONTRACT.getBytes(), Bytes.EMPTY, Bytes.of(2), WARM
            },
            new Object[] {
              "slots of an address, high bits ignored",
              0x06,
              CALLEE_WITH_HIGH_BITS,
              Bytes.EMPTY,
              Bytes.of(1),
              WARM
            },
            new Object[] {
              "slots of an unchanged address",
              0x06,
              SENDER.getBytes(),
              Bytes.EMPTY,
              Bytes.EMPTY,
              WARM
            },
            new Object[] {
              "second slot of an address", 0x07, CONTRACT.getBytes(), Bytes.of(1), Bytes.of(1), WARM
            },
            new Object[] {
              "slot of another address", 0x07, CALLEE.getBytes(), Bytes.EMPTY, Bytes.of(2), WARM
            },
            new Object[] {
              "events of an address", 0x08, CONTRACT.getBytes(), Bytes.EMPTY, Bytes.of(2), WARM
            },
            new Object[] {
              "events of another address", 0x08, CALLEE.getBytes(), Bytes.EMPTY, Bytes.of(1), WARM
            },
            new Object[] {
              "events of a silent address", 0x08, SENDER.getBytes(), Bytes.EMPTY, Bytes.EMPTY, WARM
            },
            new Object[] {
              "second event of an address",
              0x09,
              CONTRACT.getBytes(),
              Bytes.of(1),
              Bytes.of(2),
              WARM
            },
            new Object[] {
              "event of another address", 0x09, CALLEE.getBytes(), Bytes.EMPTY, Bytes.of(1), WARM
            },
            new Object[] {
              "flags of the payer: nonce and balance",
              0x0A,
              SENDER.getBytes(),
              Bytes.EMPTY,
              Bytes.of(0b0011),
              WARM
            },
            new Object[] {
              "flags of a slot writer: storage",
              0x0A,
              CONTRACT.getBytes(),
              Bytes.EMPTY,
              Bytes.of(0b0100),
              WARM
            },
            new Object[] {
              "flags of a recipient that wrote: balance and storage",
              0x0A,
              CALLEE.getBytes(),
              Bytes.EMPTY,
              Bytes.of(0b0110),
              WARM
            },
            new Object[] {
              "flags of a deployment: nonce and code",
              0x0A,
              SECOND_CALLEE.getBytes(),
              Bytes.EMPTY,
              Bytes.of(0b1001),
              WARM
            },
            new Object[] {
              "flags of an untouched address",
              0x0A,
              NEVER_EXISTED.getBytes(),
              Bytes.EMPTY,
              Bytes.EMPTY,
              WARM
            },
            new Object[] {"events with a topic", 0x0B, T2, Bytes.EMPTY, Bytes.of(2), WARM},
            new Object[] {
              "events with a topic starting with zero bytes",
              0x0B,
              T3,
              Bytes.EMPTY,
              Bytes.of(2),
              WARM
            },
            new Object[] {
              "events with a value only ever topic 0", 0x0B, T1, Bytes.EMPTY, Bytes.EMPTY, WARM
            },
            new Object[] {"first event with a topic", 0x0C, T2, Bytes.EMPTY, Bytes.EMPTY, WARM},
            // the last event carries the topic twice, and counts once
            new Object[] {"second event with a topic", 0x0C, T2, Bytes.of(1), Bytes.of(2), WARM},
            new Object[] {
              "second event with a topic starting with zero bytes",
              0x0C,
              T3,
              Bytes.of(1),
              Bytes.of(2),
              WARM
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
                                c[5])));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("txdiffValues")
  void txdiffReadsTheDiffAndTheState(
      final String evmName,
      final String name,
      final EvmConfiguration configuration,
      final int param,
      final Bytes in2,
      final Bytes in3,
      final Bytes expected,
      final long cost) {
    final ProgramRun.Outcome outcome =
        run(configuration, txdiff(Bytes.of(param), in2, in3), PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEmpty();
    assertThat(outcome.stack()).containsExactly(word(expected));
    assertThat(txdiffCosts(outcome)).containsExactly(cost);
    assertThat(outcome.remainingGas()).isEqualTo(GAS - 9 - cost);
  }

  static Stream<Arguments> txdiffHalts() {
    final Bytes twoToThe64PlusOne = Bytes.of(1, 0, 0, 0, 0, 0, 0, 0, 1);
    return Stream.of(
            // name, param, in2, in3, halt
            new Object[] {
              "balance with a reserved input",
              Bytes.of(0x02),
              SENDER.getBytes(),
              Bytes.of(1),
              "INVALID_OPERATION"
            },
            new Object[] {
              "code hash with a reserved input of 2^255",
              Bytes.of(0x05),
              CONTRACT.getBytes(),
              TWO_TO_THE_255,
              "INVALID_OPERATION"
            },
            new Object[] {
              "slots count with a reserved input",
              Bytes.of(0x06),
              CONTRACT.getBytes(),
              Bytes.of(1),
              "INVALID_OPERATION"
            },
            new Object[] {
              "events count with a reserved input of 2^64",
              Bytes.of(0x08),
              CONTRACT.getBytes(),
              TWO_TO_THE_64,
              "INVALID_OPERATION"
            },
            new Object[] {
              "flags with a reserved input",
              Bytes.of(0x0A),
              CONTRACT.getBytes(),
              Bytes.of(1),
              "INVALID_OPERATION"
            },
            new Object[] {
              "topic count with a reserved input",
              Bytes.of(0x0B),
              T2,
              Bytes.of(1),
              "INVALID_OPERATION"
            },
            new Object[] {
              "slot index past the end",
              Bytes.of(0x07),
              CONTRACT.getBytes(),
              Bytes.of(2),
              "OUT_OF_BOUNDS"
            },
            new Object[] {
              "slot index 2^64", Bytes.of(0x07), CONTRACT.getBytes(), TWO_TO_THE_64, "OUT_OF_BOUNDS"
            },
            new Object[] {
              "slot index of an unchanged address",
              Bytes.of(0x07),
              SENDER.getBytes(),
              Bytes.EMPTY,
              "OUT_OF_BOUNDS"
            },
            new Object[] {
              "event index past the end",
              Bytes.of(0x09),
              CALLEE.getBytes(),
              Bytes.of(1),
              "OUT_OF_BOUNDS"
            },
            new Object[] {
              "event index 2^255",
              Bytes.of(0x09),
              CONTRACT.getBytes(),
              TWO_TO_THE_255,
              "OUT_OF_BOUNDS"
            },
            new Object[] {
              "topic index of a value only ever topic 0",
              Bytes.of(0x0C),
              T1,
              Bytes.EMPTY,
              "OUT_OF_BOUNDS"
            },
            new Object[] {
              "topic index past the end", Bytes.of(0x0C), T2, Bytes.of(2), "OUT_OF_BOUNDS"
            },
            new Object[] {
              "undefined 0x0d",
              Bytes.of(0x0D),
              CONTRACT.getBytes(),
              Bytes.EMPTY,
              "INVALID_OPERATION"
            },
            new Object[] {
              "undefined 0xff",
              Bytes.of(0xff),
              CONTRACT.getBytes(),
              Bytes.EMPTY,
              "INVALID_OPERATION"
            },
            new Object[] {
              "undefined 2^64 + 1, which must not read as SLOT_AFTER",
              twoToThe64PlusOne,
              CONTRACT.getBytes(),
              Bytes.of(2),
              "INVALID_OPERATION"
            },
            new Object[] {
              "undefined 2^255",
              TWO_TO_THE_255,
              CONTRACT.getBytes(),
              Bytes.EMPTY,
              "INVALID_OPERATION"
            })
        .flatMap(
            c ->
                Stream.of(ProgramRun.V1, ProgramRun.V2)
                    .map(
                        v ->
                            Arguments.of(
                                v.enableEvmV2() ? "v2" : "v1", c[0], v, c[1], c[2], c[3], c[4])));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("txdiffHalts")
  void txdiffHaltsExceptionally(
      final String evmName,
      final String name,
      final EvmConfiguration configuration,
      final Bytes param,
      final Bytes in2,
      final Bytes in3,
      final String halt) {
    final ProgramRun.Outcome outcome =
        run(configuration, txdiff(param, in2, in3), PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEqualTo(halt);
    assertThat(outcome.remainingGas()).isZero();
    // a halted lookup records nothing in the block access list
    assertThat(recordedAccesses(configuration, outcome)).isEmpty();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txdiffHaltsOutsideAPostTxFrame(final String evmName, final EvmConfiguration configuration) {
    final FrameTransactionContext senderFrame = PostTxContexts.withDiff(true);
    senderFrame.setCurrentFrameIndex(1);
    for (final int param : List.of(0x00, 0x03, 0x06)) {
      final Bytes code = txdiff(Bytes.of(param), NEVER_EXISTED.getBytes(), Bytes.EMPTY);

      final ProgramRun.Outcome inSenderFrame = run(configuration, code, senderFrame);
      assertThat(inSenderFrame.haltReason()).isEqualTo("INVALID_OPERATION");
      assertThat(recordedAccesses(configuration, inSenderFrame)).isEmpty();
      // not a frame transaction: the halt consumes everything
      final ProgramRun.Outcome legacy = run(configuration, code, null);
      assertThat(legacy.haltReason()).isEqualTo("INVALID_OPERATION");
      assertThat(legacy.remainingGas()).isZero();
      // nothing installed, as before the first POST_TX frame: nothing to read
      final ProgramRun.Outcome uninstalled =
          run(configuration, code, PostTxContexts.withoutDiff(true));
      assertThat(uninstalled.haltReason()).isEqualTo("INVALID_OPERATION");
      assertThat(recordedAccesses(configuration, uninstalled)).isEmpty();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txdiffWarmsTheSlotOrTheAddressItReads(
      final String evmName, final EvmConfiguration configuration) {
    final Bytes code =
        Bytes.concatenate(
            // a cold slot, then the same slot warm: SLOT_BEFORE warms the pair for SLOT_AFTER
            txdiffCall(Bytes.of(0x00), SENDER.getBytes(), Bytes.of(3)),
            txdiffCall(Bytes.of(0x01), SENDER.getBytes(), Bytes.of(3)),
            // the slot lookups left the address cold
            txdiffCall(Bytes.of(0x03), SENDER.getBytes(), Bytes.EMPTY),
            txdiffCall(Bytes.of(0x02), SENDER.getBytes(), Bytes.EMPTY),
            // a warm address does not warm its other slots
            txdiffCall(Bytes.of(0x00), SENDER.getBytes(), Bytes.of(4)),
            // the contract is warm, but its slots are not
            txdiffCall(Bytes.of(0x01), CONTRACT.getBytes(), Bytes.EMPTY),
            // a precompile is always warm, and a view reads no state
            txdiffCall(Bytes.of(0x05), PRECOMPILE.getBytes(), Bytes.EMPTY),
            txdiffCall(Bytes.of(0x06), NEVER_EXISTED.getBytes(), Bytes.EMPTY),
            txdiffCall(Bytes.of(0x0A), NEVER_EXISTED.getBytes(), Bytes.EMPTY),
            Bytes.of(0x00));

    final ProgramRun.Outcome outcome = run(configuration, code, PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEmpty();
    assertThat(txdiffCosts(outcome))
        .containsExactly(
            COLD_SLOT, WARM, COLD_ACCOUNT, WARM, COLD_SLOT, COLD_SLOT, WARM, WARM, WARM);
    assertThat(outcome.remainingGas())
        .isEqualTo(GAS - 9 * 9 - 3 * COLD_SLOT - COLD_ACCOUNT - 5 * WARM);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txdiffWarmthOfAHaltedCallIsRolledBack(
      final String evmName, final EvmConfiguration configuration) {
    // the callee warms the sender, then halts on the reserved input
    final Bytes callee = txdiff(Bytes.of(0x02), SENDER.getBytes(), Bytes.of(1));
    // STATICCALL(10000, CALLEE, 0, 0, 0, 0); POP; then the same lookup with a zero input
    final Bytes code =
        Bytes.concatenate(
            Bytes.fromHexString("0x5f5f5f5f62ca11ee612710fa50"),
            txdiff(Bytes.of(0x02), SENDER.getBytes(), Bytes.EMPTY));

    final ProgramRun.Outcome outcome =
        ProgramRun.executeTracedInFrameTransaction(
                evm(configuration), code, callee, GAS, true, SLOTS, PostTxContexts.withDiff(true))
            .outcome();

    assertThat(outcome.haltReason()).isEmpty();
    assertThat(outcome.stack()).containsExactly(word(Bytes.ofUnsignedInt(2_000_000)));
    assertThat(txdiffCosts(outcome)).containsExactly(COLD_ACCOUNT, COLD_ACCOUNT);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txdiffRecordsStateLookupsInTheBlockAccessList(
      final String evmName, final EvmConfiguration configuration) {
    // a slot lookup records the slot, even of an account that never existed
    assertThat(
            recordedAccesses(
                configuration,
                run(
                    configuration,
                    txdiff(Bytes.of(0x00), NEVER_EXISTED.getBytes(), Bytes.of(7)),
                    PostTxContexts.withDiff(true))))
        .containsExactly(slotAccess(NEVER_EXISTED, 7));
    assertThat(
            recordedAccesses(
                configuration,
                run(
                    configuration,
                    txdiff(Bytes.of(0x01), CALLEE_WITH_HIGH_BITS, Bytes.EMPTY),
                    PostTxContexts.withDiff(true))))
        .containsExactly(slotAccess(CALLEE, 0));
    // a balance or code hash lookup records the account
    for (final int param : List.of(0x02, 0x03, 0x04, 0x05)) {
      assertThat(
              recordedAccesses(
                  configuration,
                  run(
                      configuration,
                      txdiff(Bytes.of(param), NEVER_EXISTED.getBytes(), Bytes.EMPTY),
                      PostTxContexts.withDiff(true))))
          .containsExactly(NEVER_EXISTED.toHexString());
    }
    // the views read nothing but the diff
    for (final int param : List.of(0x06, 0x07, 0x08, 0x09, 0x0A)) {
      assertThat(
              recordedAccesses(
                  configuration,
                  run(
                      configuration,
                      txdiff(Bytes.of(param), CONTRACT.getBytes(), Bytes.EMPTY),
                      PostTxContexts.withDiff(true))))
          .isEmpty();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txdiffChecksItsGasBeforeRecordingAnything(
      final String evmName, final EvmConfiguration configuration) {
    for (final Object[] lookup :
        List.of(
            new Object[] {0x00, Bytes.of(7), COLD_SLOT},
            new Object[] {0x03, Bytes.EMPTY, COLD_ACCOUNT})) {
      final Bytes code =
          txdiff(Bytes.of((Integer) lookup[0]), NEVER_EXISTED.getBytes(), (Bytes) lookup[1]);
      final long exact = 9 + (Long) lookup[2];
      final ProgramRun.Outcome enough =
          ProgramRun.executeTracedInFrameTransaction(
                  evm(configuration),
                  code,
                  Bytes.EMPTY,
                  exact,
                  true,
                  SLOTS,
                  PostTxContexts.withDiff(true))
              .outcome();
      assertThat(enough.haltReason()).isEmpty();
      assertThat(enough.remainingGas()).isZero();
      assertThat(recordedAccesses(configuration, enough)).isNotEmpty();

      final ProgramRun.Outcome oneShort =
          ProgramRun.executeTracedInFrameTransaction(
                  evm(configuration),
                  code,
                  Bytes.EMPTY,
                  exact - 1,
                  true,
                  SLOTS,
                  PostTxContexts.withDiff(true))
              .outcome();
      assertThat(oneShort.haltReason()).isEqualTo("INSUFFICIENT_GAS");
      assertThat(recordedAccesses(configuration, oneShort)).isEmpty();
    }
  }

  /**
   * v1 stack words keep the width they were pushed with, so a topic, address or key pushed with a
   * short PUSH must still match its full 32-byte value.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void txdiffReadsShortPushedOperandsAtFullWidth(
      final String evmName, final EvmConfiguration configuration) {
    final Bytes code =
        Bytes.fromHexString(
            // TXDIFF(TOPIC_EVENTS_COUNT, T3 as PUSH2, 0), T3 being 0x7f06 left-padded
            "0x5f617f06600bb7"
                // TXDIFF(TOPIC_EVENT_INDEX, T3 as PUSH2, 1)
                + "6001617f06600cb7"
                // TXDIFF(ADDRESS_SLOTS_COUNT, CONTRACT as PUSH2, 0)
                + "5f61c0de6006b7"
                // TXDIFF(SLOT_BEFORE, CONTRACT as PUSH2, 2 as PUSH1)
                + "600261c0de5fb7"
                + "00");

    final ProgramRun.Outcome outcome = run(configuration, code, PostTxContexts.withDiff(true));

    assertThat(outcome.haltReason()).isEmpty();
    assertThat(outcome.stack())
        .containsExactly(
            word(Bytes.of(5)), word(Bytes.of(2)), word(Bytes.of(2)), word(Bytes.of(2)));
  }
}
