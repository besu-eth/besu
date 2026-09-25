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

import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BlobHashOperationV2Test {

  private static final VersionedHash VERSIONED_HASH =
      VersionedHash.fromHexString(
          "0x01cafebabeb0b0facedeadbeefbeef0001cafebabeb0b0facedeadbeefbeef00");

  private final GasCalculator gasCalculator = new CancunGasCalculator();
  private final BlobHashOperationV2 operation = new BlobHashOperationV2(gasCalculator);

  @Test
  void shouldPushVersionedHash() {
    final MessageFrame frame = createFrame(Bytes32.ZERO, Optional.of(List.of(VERSIONED_HASH)));

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getVeryLowTierGasCost());
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(VERSIONED_HASH.getBytes().toArrayUnsafe()));
    assertThat(frame.stackTopV2()).isEqualTo(1);
  }

  @Test
  void shouldPushZeroWhenTransactionHasNoVersionedHashes() {
    final MessageFrame absent = createFrame(Bytes32.ZERO, Optional.empty());
    final MessageFrame empty = createFrame(Bytes32.ZERO, Optional.of(List.of()));

    operation.execute(absent, null);
    operation.execute(empty, null);

    assertThat(getV2StackItem(absent, 0)).isEqualTo(UInt256.ZERO);
    assertThat(getV2StackItem(empty, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushZeroWhenIndexIsOutOfBounds() {
    final MessageFrame frame =
        createFrame(Bytes32.fromHexString("0x01"), Optional.of(List.of(VERSIONED_HASH)));

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushZeroWhenIndexDoesNotFitLong() {
    final MessageFrame frame =
        createFrame(
            Bytes32.fromHexString("0x010000000000000000"), Optional.of(List.of(VERSIONED_HASH)));

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldHaltOnInsufficientGasWithoutChangingStack() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .initialGas(1)
            .versionedHashes(Optional.of(List.of(VERSIONED_HASH)))
            .pushStackItem(Bytes32.ZERO)
            .build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldHaltOnStackUnderflow() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame createFrame(
      final Bytes32 index, final Optional<List<VersionedHash>> versionedHashes) {
    return new TestMessageFrameBuilderV2()
        .versionedHashes(versionedHashes)
        .pushStackItem(index)
        .build();
  }
}
