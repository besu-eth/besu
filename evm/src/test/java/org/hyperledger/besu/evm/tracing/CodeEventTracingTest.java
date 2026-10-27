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
package org.hyperledger.besu.evm.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The code events the EVM emits, and where: each at the point the code is read or written. */
class CodeEventTracingTest {

  private static final Address CALLER = Address.fromHexString("0x1000");
  private static final Address CONTRACT = Address.fromHexString("0x2000");
  private static final Address TARGET = Address.fromHexString("0x3000");
  private static final Address DELEGATED = Address.fromHexString("0x4000");

  private static final Bytes TARGET_CODE = Bytes.fromHexString("0x00");
  private static final Bytes DESIGNATOR =
      Bytes.concatenate(Bytes.fromHexString("0xef0100"), TARGET.getBytes());

  // gas, to, value, in offset, in size, out offset, out size: seven pushes of 3 gas each.
  private static final long CALL_ARGS_GAS = 7 * 3;
  // Prague: a cold `to`, then the cold delegation target resolved from its designator.
  private static final long COLD_ACCESS = 2600;

  // Creates a contract whose initcode deposits the single byte 0xfe, then reverts.
  private static final Bytes CREATE_THEN_REVERT =
      Bytes.fromHexString(
          "0x69"
              + "60fe60005360016000f3" // initcode returning 0xfe
              + "600052" // PUSH10 initcode, MSTORE at 0 (right-aligned: 22..31)
              + "600a60166000f0" // CREATE(value 0, offset 22, size 10)
              + "60006000fd"); // REVERT

  private SimpleWorld world;
  private RecordingTracer tracer;

  @BeforeEach
  void setUp() {
    world = new SimpleWorld();
    world.createAccount(CALLER, 0, Wei.of(1_000_000));
    world.createAccount(CONTRACT, 1, Wei.ZERO);
    world.createAccount(TARGET, 0, Wei.ZERO).setCode(TARGET_CODE);
    world.createAccount(DELEGATED, 0, Wei.ZERO).setCode(DESIGNATOR);
    tracer = new RecordingTracer(true);
  }

  @Test
  void tracerNotOptedInReceivesNoCodeEvents() {
    tracer = new RecordingTracer(false);

    run(Bytes.fromHexString("0x73" + TARGET.getBytes().toUnprefixedHexString() + "3b00"), 100_000);
    run(callCode(DELEGATED), 100_000);
    run(CREATE_THEN_REVERT, 1_000_000);

    assertThat(tracer.events).isNotEmpty();
    assertThat(tracer.events).noneMatch(e -> e.startsWith("read ") || e.startsWith("write "));
  }

  @Test
  void extCodeSizeReadsTheAccountsCode() {
    run(Bytes.fromHexString("0x73" + TARGET.getBytes().toUnprefixedHexString() + "3b00"), 100_000);

    assertThat(tracer.reads).contains(read(TARGET, TARGET_CODE));
  }

  @Test
  void extCodeSizeOutOfGasBeforeTheLoadReadsNothing() {
    run(Bytes.fromHexString("0x73" + TARGET.getBytes().toUnprefixedHexString() + "3b00"), 3 + 100);

    assertThat(tracer.reads).doesNotContain(read(TARGET, TARGET_CODE));
  }

  @Test
  void extCodeCopyReadsTheAccountsCode() {
    // size, source offset, dest offset, address
    run(
        Bytes.fromHexString(
            "0x600160006000" + "73" + TARGET.getBytes().toUnprefixedHexString() + "3c00"),
        100_000);

    assertThat(tracer.reads).contains(read(TARGET, TARGET_CODE));
  }

  @Test
  void callToDelegatedAccountReadsDesignatorAndTarget() {
    run(callCode(DELEGATED), 100_000);

    assertThat(tracer.reads).contains(read(DELEGATED, DESIGNATOR), read(TARGET, TARGET_CODE));
  }

  @Test
  void callOutOfGasAfterDelegationResolutionReadsOnlyTheDesignator() {
    // Enough for the call's own cold access, not for the delegation target's.
    run(callCode(DELEGATED), CALL_ARGS_GAS + COLD_ACCESS + 100);

    assertThat(tracer.reads).contains(read(DELEGATED, DESIGNATOR));
    assertThat(tracer.reads).doesNotContain(read(TARGET, TARGET_CODE));
  }

  @Test
  void callOutOfGasBeforeDelegationResolutionReadsNothing() {
    run(callCode(DELEGATED), CALL_ARGS_GAS + COLD_ACCESS - 100);

    assertThat(tracer.reads).doesNotContain(read(DELEGATED, DESIGNATOR));
    assertThat(tracer.reads).doesNotContain(read(TARGET, TARGET_CODE));
  }

  @Test
  void callWithInsufficientBalanceStillReadsTheDelegationTarget() {
    // CONTRACT holds no balance, so the call soft-fails before any child frame runs.
    run(callCode(DELEGATED, 1), 100_000);

    assertThat(tracer.events).noneMatch(e -> e.startsWith("exit 1 "));
    assertThat(tracer.reads).contains(read(DELEGATED, DESIGNATOR), read(TARGET, TARGET_CODE));
  }

  @Test
  void callToPlainContractReadsItsCode() {
    run(callCode(TARGET), 100_000);

    assertThat(tracer.reads).contains(read(TARGET, TARGET_CODE));
  }

  @Test
  void codeDepositIsWrittenThenTheRevertingParentExitsFailed() {
    run(CREATE_THEN_REVERT, 1_000_000);

    final Hash depositedHash = Hash.hash(Bytes.fromHexString("0xfe"));
    final int write = indexOf(tracer.events, "write ", " " + depositedHash);
    assertThat(write).isNotNegative();
    assertThat(tracer.events.subList(write, tracer.events.size()))
        .contains("exit 1 COMPLETED_SUCCESS", "exit 0 COMPLETED_FAILED");
    assertThat(tracer.events.getLast()).isEqualTo("exit 0 COMPLETED_FAILED");
  }

  private static int indexOf(final List<String> events, final String prefix, final String suffix) {
    for (int i = 0; i < events.size(); i++) {
      if (events.get(i).startsWith(prefix) && events.get(i).endsWith(suffix)) {
        return i;
      }
    }
    return -1;
  }

  private static Bytes callCode(final Address to) {
    return callCode(to, 0);
  }

  private static Bytes callCode(final Address to, final int value) {
    return Bytes.fromHexString(
        "0x6000600060006000"
            + String.format("60%02x", value)
            + "73"
            + to.getBytes().toUnprefixedHexString()
            + "6000"
            + "f1"
            + "00");
  }

  private void run(final Bytes code, final long gas) {
    new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.PRAGUE))
        .worldUpdater(world.updater())
        .sender(CALLER)
        .receiver(CONTRACT)
        .contract(CONTRACT)
        .gas(gas)
        .tracer(tracer)
        .execute(code, Bytes.EMPTY, Wei.ZERO, CONTRACT);
  }

  private static String read(final Address address, final Bytes code) {
    return address + " " + Hash.hash(code);
  }

  private static final class RecordingTracer implements OperationTracer {
    final boolean codeAccessTracing;
    final List<String> reads = new ArrayList<>();
    final List<String> events = new ArrayList<>();

    RecordingTracer(final boolean codeAccessTracing) {
      this.codeAccessTracing = codeAccessTracing;
    }

    @Override
    public boolean isCodeAccessTracing() {
      return codeAccessTracing;
    }

    @Override
    public void traceCodeRead(final Address address, final Hash codeHash) {
      reads.add(address + " " + codeHash);
      events.add("read " + address + " " + codeHash);
    }

    @Override
    public void traceCodeWrite(final Address address, final Hash codeHash) {
      events.add("write " + address + " " + codeHash);
    }

    @Override
    public void traceContextExit(final MessageFrame frame) {
      events.add("exit " + frame.getDepth() + " " + frame.getState());
    }
  }
}
