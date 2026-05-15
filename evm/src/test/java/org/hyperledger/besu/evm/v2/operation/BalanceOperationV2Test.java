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
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BalanceOperationV2Test {

  private static final Address TARGET = Address.fromHexString("0xABCDEF0123456789ABCD");
  private final GasCalculator gasCalculator = new BerlinGasCalculator();
  private final BalanceOperationV2 operation = new BalanceOperationV2(gasCalculator);

  @Test
  void shouldPushBalanceToStack() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(TARGET, 0, Wei.of(99999L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(world)
            .pushStackItem(Bytes32.leftPad(TARGET.getBytes()))
            .build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 99999L));
  }

  @Test
  void shouldPushZeroForNonExistentAccount() {
    final ToyWorld world = new ToyWorld();
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(world)
            .pushStackItem(Bytes32.leftPad(TARGET.getBytes()))
            .build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushLargeBalance() {
    final Wei largeBalance =
        Wei.fromHexString("0x00000000000000010000000000000002000000000000000300000000000000FF");
    final ToyWorld world = new ToyWorld();
    world.createAccount(TARGET, 0, largeBalance);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(world)
            .pushStackItem(Bytes32.leftPad(TARGET.getBytes()))
            .build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(
            new UInt256(
                0x0000000000000001L,
                0x0000000000000002L,
                0x0000000000000003L,
                0x00000000000000FFL));
  }

  @Test
  void shouldChargeColdAccessCost() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(TARGET, 0, Wei.of(1L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(world)
            .pushStackItem(Bytes32.leftPad(TARGET.getBytes()))
            .build();
    final OperationResult result = operation.execute(frame, null);
    final long expectedCost =
        gasCalculator.getBalanceOperationGasCost() + gasCalculator.getColdAccountAccessCost();
    assertThat(result.getGasCost()).isEqualTo(expectedCost);
    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void shouldChargeWarmAccessCost() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(TARGET, 0, Wei.of(1L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(world)
            .pushStackItem(Bytes32.leftPad(TARGET.getBytes()))
            .build();
    frame.warmUpAddress(TARGET);
    final OperationResult result = operation.execute(frame, null);
    final long expectedCost =
        gasCalculator.getBalanceOperationGasCost() + gasCalculator.getWarmStorageReadCost();
    assertThat(result.getGasCost()).isEqualTo(expectedCost);
    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(TARGET, 0, Wei.of(1L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .initialGas(1)
            .worldUpdater(world)
            .pushStackItem(Bytes32.leftPad(TARGET.getBytes()))
            .build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackUnderflow() {
    final ToyWorld world = new ToyWorld();
    final MessageFrame frame = new TestMessageFrameBuilderV2().worldUpdater(world).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }
}
