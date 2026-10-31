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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ExtCodeCopyOperationV2Test {

  private static final Address ACCOUNT = Address.fromHexString("0xc0de");
  private static final Bytes CODE = Bytes.fromHexString("0x60016002016003");

  private final WorldUpdater worldUpdater = new ToyWorld().updater();
  private final ExtCodeCopyOperationV2 operation =
      new ExtCodeCopyOperationV2(new BerlinGasCalculator());

  @Test
  void copiesTheCodeOfTheAccount() {
    worldUpdater.getOrCreate(ACCOUNT).setCode(CODE);
    final MessageFrame frame = frame(ACCOUNT, "0x01", "0x03", "0x06");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isZero();
    assertThat(frame.readMemory(0, 8)).isEqualTo(Bytes.fromHexString("0x0002016003000000"));
    // 3 for the word copied, 3 for the word of memory and 2600 for the cold account
    assertThat(result.getGasCost()).isEqualTo(2606);
  }

  @Test
  void chargesAWarmAccountLess() {
    worldUpdater.getOrCreate(ACCOUNT).setCode(CODE);
    final MessageFrame frame = frame(ACCOUNT, "0x00", "0x00", "0x00");
    frame.warmUpAddress(ACCOUNT);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(100);
  }

  @Test
  void copiesZerosForAMissingAccount() {
    final MessageFrame frame = frame(ACCOUNT, "0x00", "0x00", "0x04");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.readMemory(0, 4)).isEqualTo(Bytes.wrap(new byte[4]));
  }

  @Test
  void haltsWithoutGasForTheColdAccess() {
    worldUpdater.getOrCreate(ACCOUNT).setCode(CODE);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(worldUpdater)
            .initialGas(2605)
            .pushStackItem(Bytes32.fromHexString("0x06"))
            .pushStackItem(Bytes32.fromHexString("0x03"))
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .pushStackItem(Bytes32.leftPad(ACCOUNT.getBytes()))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame frame(
      final Address address,
      final String memOffset,
      final String sourceOffset,
      final String length) {
    return new TestMessageFrameBuilderV2()
        .worldUpdater(worldUpdater)
        .initialGas(1_000_000)
        .pushStackItem(Bytes32.fromHexString(length))
        .pushStackItem(Bytes32.fromHexString(sourceOffset))
        .pushStackItem(Bytes32.fromHexString(memOffset))
        .pushStackItem(Bytes32.leftPad(address.getBytes()))
        .build();
  }
}
