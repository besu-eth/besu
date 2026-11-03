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
import static org.hyperledger.besu.evm.v2.ProgramRun.SECOND_CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.v2.ProgramRun.Execution;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Runs the call operations of EVM v2 through child frames, as a transaction does. */
class CallOperationsV2Test {

  private static final EVM EVM_V2 = MainnetEVMs.cancun(ProgramRun.V2);

  /**
   * Returns the call value, the caller and its own address as 96 bytes, and stores 1 in slot 0 when
   * it is called without value.
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

  /** Returns the call value, the caller and its own address as 96 bytes. */
  private static final Bytes OBSERVING_CODE =
      Bytes.fromHexString("0x34600052336020523060405260606000f3");

  private static final int CALL = 0xf1;
  private static final int CALLCODE = 0xf2;
  private static final int DELEGATECALL = 0xf4;
  private static final int STATICCALL = 0xfa;

  @Test
  void callPushesOneAndCopiesTheOutput() {
    final Execution execution = execute(call(CALL, CALLEE, 0), false);

    assertThat(execution.outcome().haltReason()).isEmpty();
    assertThat(execution.outcome().stack()).containsExactly(stackItem(UInt256.ONE));
    assertThat(execution.frame().readMemory(0, 32)).isEqualTo(Bytes32.ZERO);
    assertThat(execution.frame().getReturnData())
        .isEqualTo(Bytes.concatenate(Bytes32.ZERO, word(CONTRACT), word(CALLEE)));
    assertThat(execution.worldUpdater().get(CALLEE).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ONE);
  }

  @Test
  void callTransfersTheValue() {
    final Execution execution = execute(call(CALL, CALLEE, 7), false);

    assertThat(execution.outcome().stack()).containsExactly(stackItem(UInt256.ONE));
    assertThat(execution.worldUpdater().get(CALLEE).getBalance()).isEqualTo(Wei.of(7));
    assertThat(execution.frame().readMemory(0, 32)).isEqualTo(Bytes32.leftPad(Bytes.of(7)));
  }

  @Test
  void callWithMoreValueThanTheBalancePushesZero() {
    final Execution execution = execute(call(CALL, CALLEE, 2_000_000), false);

    assertThat(execution.outcome().haltReason()).isEmpty();
    assertThat(execution.outcome().stack()).containsExactly(stackItem(UInt256.ZERO));
    assertThat(execution.frame().getReturnData()).isEqualTo(Bytes.EMPTY);
    assertThat(execution.worldUpdater().get(CALLEE).getBalance()).isEqualTo(Wei.ZERO);
  }

  @Test
  void delegateCallRunsTheCodeInTheCallersContext() {
    final Execution execution = execute(call(DELEGATECALL, CALLEE, 0), false);

    assertThat(execution.outcome().stack()).containsExactly(stackItem(UInt256.ONE));
    // the call value of the contract's own call, its sender, and the contract
    assertThat(execution.frame().getReturnData())
        .isEqualTo(
            Bytes.concatenate(
                Bytes32.leftPad(ProgramRun.CALL_VALUE.toMinimalBytes()),
                word(SENDER),
                word(CONTRACT)));
  }

  @Test
  void callCodeRunsTheCodeOnTheCallersAccount() {
    final Execution execution = execute(call(CALLCODE, CALLEE, 0), false);

    assertThat(execution.outcome().stack()).containsExactly(stackItem(UInt256.ONE));
    assertThat(execution.frame().getReturnData())
        .isEqualTo(Bytes.concatenate(Bytes32.ZERO, word(CONTRACT), word(CONTRACT)));
    assertThat(execution.worldUpdater().get(CONTRACT).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ONE);
    assertThat(execution.worldUpdater().get(CALLEE).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
  }

  @Test
  void staticCallPushesZeroWhenTheCalleeChangesState() {
    final Execution execution = execute(call(STATICCALL, CALLEE, 0), false);

    assertThat(execution.outcome().haltReason()).isEmpty();
    assertThat(execution.outcome().stack()).containsExactly(stackItem(UInt256.ZERO));
    assertThat(execution.worldUpdater().get(CALLEE).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
  }

  @Test
  void staticCallRunsCodeThatDoesNotChangeState() {
    final Execution execution = execute(call(STATICCALL, SECOND_CALLEE, 0), false);

    assertThat(execution.outcome().stack()).containsExactly(stackItem(UInt256.ONE));
    assertThat(execution.frame().getReturnData())
        .isEqualTo(Bytes.concatenate(Bytes32.ZERO, word(CONTRACT), word(SECOND_CALLEE)));
  }

  @Test
  void callWithValueHaltsInAStaticFrame() {
    final Execution execution = execute(call(CALL, CALLEE, 1), true);

    assertThat(execution.outcome().haltReason())
        .isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE.name());
  }

  @Test
  void callHaltsOnStackUnderflow() {
    // six of the seven arguments of CALL
    final Execution execution =
        execute(Bytes.fromHexString("0x600060006000600060006000f100"), false);

    assertThat(execution.outcome().haltReason())
        .isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS.name());
  }

  private static Execution execute(final Bytes code, final boolean isStatic) {
    return ProgramRun.execute(
        EVM_V2, code, CALLEE_CODE, OBSERVING_CODE, 1_000_000L, isStatic, List.of(Bytes32.ZERO));
  }

  /** The code of a call to the address, writing the first word of its output to memory 0. */
  private static Bytes call(final int opcode, final Address to, final long value) {
    final boolean takesValue = opcode == CALL || opcode == CALLCODE;
    return Bytes.concatenate(
        // out size, out offset, in size, in offset
        Bytes.fromHexString("0x6020600060006000"),
        takesValue
            ? Bytes.concatenate(Bytes.of(0x7f), Bytes32.leftPad(Bytes.ofUnsignedLong(value)))
            : Bytes.EMPTY,
        Bytes.of(0x73),
        to.getBytes(),
        // gas
        Bytes.fromHexString("0x620fffff"),
        Bytes.of(opcode, 0x00));
  }

  private static Bytes32 word(final Address address) {
    return Bytes32.leftPad(address.getBytes());
  }

  /** A stack item as the outcome lists it. */
  private static String stackItem(final UInt256 value) {
    return value.toUnprefixedHexString();
  }
}
