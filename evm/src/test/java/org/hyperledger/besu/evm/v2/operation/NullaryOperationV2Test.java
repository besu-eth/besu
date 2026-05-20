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

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.Optional;

import org.junit.jupiter.api.Test;

abstract class NullaryOperationV2Test {

  protected final Operation operation;

  public NullaryOperationV2Test(final Operation operation) {
    this.operation = operation;
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().initialGas(1).build();
    assertThat(frame.stackTopV2()).isEqualTo(0);
    final Operation.OperationResult result = operation.execute(frame, null);
    assertThat(frame.stackTopV2()).isEqualTo(0); // OOG before stack is touched
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    Wei baseFee = Wei.of(5L);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            // required for BaseFeeOperation
            .blockValues(new FakeBlockValues(Optional.of(baseFee)))
            .build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final Operation.OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }

  @Test
  void shouldHaltOnInsufficientGasEvenStackOverflow() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().initialGas(1L).build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final Operation.OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }
}
