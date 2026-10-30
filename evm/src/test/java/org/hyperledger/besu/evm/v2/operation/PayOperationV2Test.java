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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class PayOperationV2Test {

  private static final Address CURRENT = Address.fromHexString("0x5e4d");
  private static final Address RECIPIENT = Address.fromHexString("0x4ec1");

  private final WorldUpdater worldUpdater = new ToyWorld().updater();
  private final PayOperationV2 operation = new PayOperationV2(new AmsterdamGasCalculator());

  @Test
  void paysTheValueAndPushesOne() {
    worldUpdater.getOrCreate(CURRENT).setBalance(Wei.of(1_000));
    final MessageFrame frame = frame(Bytes32.leftPad(RECIPIENT.getBytes()), 300);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ONE);
    assertThat(worldUpdater.get(CURRENT).getBalance()).isEqualTo(Wei.of(700));
    assertThat(worldUpdater.get(RECIPIENT).getBalance()).isEqualTo(Wei.of(300));
  }

  @Test
  void paysFromTheCurrentAddressNotFromTheCaller() {
    final Address caller = Address.fromHexString("0xca11");
    worldUpdater.getOrCreate(caller).setBalance(Wei.of(5_000));
    worldUpdater.getOrCreate(CURRENT).setBalance(Wei.of(1_000));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(worldUpdater)
            .sender(caller)
            .address(CURRENT)
            .initialGas(100_000L)
            .pushStackItem(Bytes32.leftPad(Bytes.ofUnsignedLong(300)))
            .pushStackItem(Bytes32.leftPad(RECIPIENT.getBytes()))
            .build();

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ONE);
    assertThat(worldUpdater.get(CURRENT).getBalance()).isEqualTo(Wei.of(700));
    assertThat(worldUpdater.get(caller).getBalance()).isEqualTo(Wei.of(5_000));
  }

  @Test
  void payingItselfMoreThanItsBalancePushesZero() {
    worldUpdater.getOrCreate(CURRENT).setBalance(Wei.of(100));
    final MessageFrame frame = frame(Bytes32.leftPad(CURRENT.getBytes()), 300);

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
    assertThat(worldUpdater.get(CURRENT).getBalance()).isEqualTo(Wei.of(100));
  }

  @Test
  void pushesZeroWithoutTheBalance() {
    worldUpdater.getOrCreate(CURRENT).setBalance(Wei.of(1));
    final MessageFrame frame = frame(Bytes32.leftPad(RECIPIENT.getBytes()), 300);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
    assertThat(worldUpdater.get(CURRENT).getBalance()).isEqualTo(Wei.of(1));
  }

  @Test
  void haltsOnARecipientBeyondTwentyBytes() {
    final MessageFrame frame = frame(Bytes32.fromHexString("0x01" + "00".repeat(20)), 300);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ADDRESS_OUT_OF_RANGE);
  }

  @Test
  void haltsInAStaticFrame() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(worldUpdater)
            .address(CURRENT)
            .isStatic(true)
            .pushStackItem(Bytes32.leftPad(Bytes.of(1)))
            .pushStackItem(Bytes32.leftPad(RECIPIENT.getBytes()))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(worldUpdater)
            .pushStackItem(Bytes32.leftPad(RECIPIENT.getBytes()))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame frame(final Bytes32 recipient, final long value) {
    return new TestMessageFrameBuilderV2()
        .worldUpdater(worldUpdater)
        .address(CURRENT)
        .initialGas(100_000L)
        .pushStackItem(Bytes32.leftPad(Bytes.ofUnsignedLong(value)))
        .pushStackItem(recipient)
        .build();
  }
}
