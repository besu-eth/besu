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
import static org.hyperledger.besu.evm.frame.ExceptionalHaltReason.INSUFFICIENT_GAS;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.gascalculator.Eip8037StateGasCostCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.LondonGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SStoreOperation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SStoreOperationV2Test {

  private static final Address ADDRESS = Address.fromHexString("0x18675309");
  private static final UInt256 KEY =
      UInt256.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");

  private final WorldUpdater worldUpdater = new ToyWorld().updater();

  static Iterable<Arguments> minimumGas() {
    return List.of(
        Arguments.of(SStoreOperation.FRONTIER_MINIMUM, 200L, null),
        Arguments.of(SStoreOperation.EIP_1706_MINIMUM, 200L, INSUFFICIENT_GAS),
        Arguments.of(SStoreOperation.FRONTIER_MINIMUM, 10_000L, null),
        Arguments.of(SStoreOperation.EIP_1706_MINIMUM, 10_000L, null),
        Arguments.of(SStoreOperation.EIP_1706_MINIMUM, 2300L, INSUFFICIENT_GAS),
        Arguments.of(SStoreOperation.EIP_1706_MINIMUM, 2301L, null));
  }

  @ParameterizedTest(name = "{index}: minimum gas {0}, remaining gas {1}, expected halt {2}")
  @MethodSource("minimumGas")
  void keepsTheMinimumGas(
      final long minimumGas, final long remainingGas, final ExceptionalHaltReason expectedHalt) {
    // storing the value the slot holds costs 200 on Constantinople
    final MessageFrame frame = frame(remainingGas, "0x00");

    final Operation.OperationResult result =
        new SStoreOperationV2(new ConstantinopleGasCalculator(), minimumGas).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(expectedHalt);
  }

  @Test
  void storesTheValueAndPopsBothItems() {
    final GasCalculator gasCalculator = new LondonGasCalculator();
    final MessageFrame frame = frame(100_000L, "0x2a");

    final Operation.OperationResult result =
        new SStoreOperationV2(gasCalculator, SStoreOperation.EIP_1706_MINIMUM).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isZero();
    assertThat(worldUpdater.get(ADDRESS).getStorageValue(KEY)).isEqualTo(UInt256.valueOf(0x2a));
    // a cold zero slot set to a non-zero value: 20000 for the set and 2100 for the cold access
    assertThat(result.getGasCost()).isEqualTo(22_100L);
  }

  @Test
  void refundsClearingASlot() {
    final GasCalculator gasCalculator = new LondonGasCalculator();
    worldUpdater.getOrCreate(ADDRESS).setStorageValue(KEY, UInt256.ONE);
    worldUpdater.commit();
    final MessageFrame frame = frame(100_000L, "0x00");

    new SStoreOperationV2(gasCalculator, SStoreOperation.EIP_1706_MINIMUM).execute(frame, null);

    assertThat(frame.getGasRefund()).isEqualTo(4800L);
  }

  @Test
  void haltsInAStaticFrame() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .address(ADDRESS)
            .worldUpdater(worldUpdater)
            .isStatic(true)
            .pushStackItem(Bytes32.fromHexString("0x2a"))
            .pushStackItem(KEY)
            .build();

    final Operation.OperationResult result =
        new SStoreOperationV2(new LondonGasCalculator(), SStoreOperation.EIP_1706_MINIMUM)
            .execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
  }

  @Test
  void chargesStateGasForANewSlotOnAmsterdam() {
    final GasCalculator gasCalculator = new AmsterdamGasCalculator();
    final MessageFrame frame = frame(200_000L, "0x2a");

    final Operation.OperationResult result =
        new SStoreOperationV2(gasCalculator, SStoreOperation.EIP_1706_MINIMUM).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getStateGasUsed())
        .isEqualTo(new Eip8037StateGasCostCalculator().storageSetStateGas());
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .address(ADDRESS)
            .worldUpdater(worldUpdater)
            .pushStackItem(KEY)
            .build();

    final Operation.OperationResult result =
        new SStoreOperationV2(new LondonGasCalculator(), SStoreOperation.EIP_1706_MINIMUM)
            .execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame frame(final long remainingGas, final String value) {
    worldUpdater.getOrCreate(ADDRESS).setBalance(Wei.of(1));
    worldUpdater.commit();
    return new TestMessageFrameBuilderV2()
        .address(ADDRESS)
        .worldUpdater(worldUpdater)
        .initialGas(remainingGas)
        .pushStackItem(Bytes32.fromHexString(value))
        .pushStackItem(KEY)
        .build();
  }
}
