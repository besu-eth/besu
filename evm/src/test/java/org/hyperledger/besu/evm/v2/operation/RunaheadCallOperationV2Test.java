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
package org.hyperledger.besu.evm.v2.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2.getV2StackItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.RunaheadCalls;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RunaheadCallOperationV2Test {

  private static final Address TARGET =
      Address.fromHexString("0x00000000000000000000000000000000000c0de");
  private static final Bytes INPUT = Bytes.fromHexString("0xc40493dc");
  private static final Bytes PREDICTED =
      Bytes32.fromHexString("0x000000000000000000000000000000000000000000000000000000000000002a");

  private final StaticCallOperationV2 operation =
      new StaticCallOperationV2(new PragueGasCalculator());

  @AfterEach
  void tearDown() {
    RunaheadCalls.CURRENT.remove();
  }

  @Test
  void aRunAheadSkipsACallToAPlaceholderAndPushesThePredictedOutput() {
    final Account placeholder = mock(Account.class);
    when(placeholder.getCodeHash()).thenReturn(RunaheadCalls.PLACEHOLDER_CODE_HASH);
    final WorldUpdater world = mock(WorldUpdater.class);
    when(world.get(TARGET)).thenReturn(placeholder);
    final RecordingCalls calls = new RecordingCalls();
    RunaheadCalls.Switch.enable();
    RunaheadCalls.CURRENT.set(calls);

    final MessageFrame frame = staticCallFrame(world);
    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(1);
    assertThat(calls.lookedUpTargetInMarkedLookup).isTrue();
    assertThat(calls.lastInput).isEqualTo(INPUT);
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ONE);
    assertThat(frame.getReturnData()).isEqualTo(PREDICTED);
    assertThat(frame.readMemory(64, 32)).isEqualTo(PREDICTED);
    // no child frame runs the placeholder
    assertThat(frame.getMessageFrameStack()).hasSize(1);
  }

  @Test
  void aRealExecutionCallsAPlaceholderLikeAnyAccount() {
    final Account account = mock(Account.class);
    when(account.getCodeHash()).thenReturn(Hash.EMPTY);
    when(account.getCode()).thenReturn(Bytes.EMPTY);
    final WorldUpdater world = mock(WorldUpdater.class);
    when(world.get(TARGET)).thenReturn(account);

    assertThat(RunaheadCalls.current()).isNull();
    final MessageFrame frame = staticCallFrame(world);
    frame.getMessageFrameStack().push(frame);
    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // the call is set up as a child frame rather than answered in place
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_SUSPENDED);
  }

  private static MessageFrame staticCallFrame(final WorldUpdater world) {
    // STATICCALL pops gas, to, input offset, input size, output offset, output size
    return new TestMessageFrameBuilderV2()
        .worldUpdater(world)
        .initialGas(1_000_000L)
        .memory(Bytes.concatenate(INPUT, Bytes.wrap(new byte[60])))
        .pushStackItem(Bytes32.leftPad(Bytes.of(32)))
        .pushStackItem(Bytes32.leftPad(Bytes.of(64)))
        .pushStackItem(Bytes32.leftPad(Bytes.of(4)))
        .pushStackItem(Bytes32.ZERO)
        .pushStackItem(Bytes32.leftPad(TARGET.getBytes()))
        .pushStackItem(Bytes32.leftPad(Bytes.fromHexString("0x0186a0")))
        .build();
  }

  private static final class RecordingCalls implements RunaheadCalls {
    private boolean lookingUp;
    private boolean lookedUpTargetInMarkedLookup;
    private Bytes lastInput;

    @Override
    public void lookingUpCallTarget(final boolean lookingUp) {
      this.lookingUp = lookingUp;
      if (lookingUp) {
        lookedUpTargetInMarkedLookup = true;
      }
    }

    @Override
    public boolean isLookingUpCallTarget() {
      return lookingUp;
    }

    @Override
    public Bytes predictedReturn(final Bytes input) {
      lastInput = input.copy();
      return PREDICTED;
    }

    @Override
    public void recordReturn(final Bytes input, final Bytes output) {}
  }
}
