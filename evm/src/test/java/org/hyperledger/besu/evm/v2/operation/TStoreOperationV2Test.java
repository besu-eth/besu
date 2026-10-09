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
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class TStoreOperationV2Test {

  private static final Address ADDRESS = Address.fromHexString("0x18675309");
  private static final Bytes32 KEY =
      Bytes32.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");
  private static final Bytes32 VALUE =
      Bytes32.fromHexString("0xf0e1d2c3b4a5968778695a4b3c2d1e0f0123456789abcdeffedcba9876543210");

  private final TStoreOperationV2 operation = new TStoreOperationV2(new CancunGasCalculator());

  @Test
  void storesTheValueForTheRecipient() {
    final MessageFrame frame = frame(false, 1_000L);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(100L);
    assertThat(frame.stackTopV2()).isZero();
    assertThat(frame.getTransientStorageValue(ADDRESS, KEY)).isEqualTo(VALUE);
    assertThat(frame.getTransientStorageValue(ADDRESS, VALUE)).isEqualTo(Bytes32.ZERO);
  }

  @Test
  void haltsInAStaticFrame() {
    final MessageFrame frame = frame(true, 1_000L);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    assertThat(frame.getTransientStorageValue(ADDRESS, KEY)).isEqualTo(Bytes32.ZERO);
  }

  @Test
  void haltsWithoutGas() {
    final MessageFrame frame = frame(false, 99L);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(frame.getTransientStorageValue(ADDRESS, KEY)).isEqualTo(Bytes32.ZERO);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().address(ADDRESS).pushStackItem(KEY).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private static MessageFrame frame(final boolean isStatic, final long gas) {
    return new TestMessageFrameBuilderV2()
        .address(ADDRESS)
        .isStatic(isStatic)
        .initialGas(gas)
        .pushStackItem(VALUE)
        .pushStackItem(KEY)
        .build();
  }
}
