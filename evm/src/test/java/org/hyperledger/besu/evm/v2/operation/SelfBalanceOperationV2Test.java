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

import org.junit.jupiter.api.Test;

class SelfBalanceOperationV2Test {

  private static final Address RECIPIENT = Address.fromHexString("0xdeadbeef");
  private final GasCalculator gasCalculator = new BerlinGasCalculator();
  private final SelfBalanceOperationV2 operation = new SelfBalanceOperationV2(gasCalculator);

  @Test
  void shouldPushSelfBalanceToStack() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(RECIPIENT, 0, Wei.of(12345L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().worldUpdater(world).address(RECIPIENT).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 12345L));
  }

  @Test
  void shouldPushZeroForNullAccount() {
    final ToyWorld world = new ToyWorld();
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().worldUpdater(world).address(RECIPIENT).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushLargeBalance() {
    final Wei largeBalance =
        Wei.fromHexString("0x00000000000000010000000000000002000000000000000300000000000000FF");
    final ToyWorld world = new ToyWorld();
    world.createAccount(RECIPIENT, 0, largeBalance);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().worldUpdater(world).address(RECIPIENT).build();
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
  void shouldReturnCorrectGasCost() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(RECIPIENT, 0, Wei.of(1L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().worldUpdater(world).address(RECIPIENT).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getLowTierGasCost());
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(RECIPIENT, 0, Wei.of(1L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .initialGas(4)
            .worldUpdater(world)
            .address(RECIPIENT)
            .build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    final ToyWorld world = new ToyWorld();
    world.createAccount(RECIPIENT, 0, Wei.of(1L));
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().worldUpdater(world).address(RECIPIENT).build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }
}
