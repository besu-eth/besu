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
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class SLoadOperationV2Test {

  private static final Address ADDRESS = Address.fromHexString("0x18675309");
  private static final UInt256 KEY =
      UInt256.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");
  private static final UInt256 VALUE =
      UInt256.fromHexString("0xf0e1d2c3b4a5968778695a4b3c2d1e0f0123456789abcdeffedcba9876543210");

  private final WorldUpdater worldUpdater = new ToyWorld().updater();
  private final SLoadOperationV2 operation = new SLoadOperationV2(new BerlinGasCalculator());

  @Test
  void loadsTheSlotAndChargesTheColdAccess() {
    worldUpdater.getOrCreate(ADDRESS).setStorageValue(KEY, VALUE);
    final MessageFrame frame = frame();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(org.hyperledger.besu.evm.UInt256.fromBytesBE(VALUE.toArrayUnsafe()));
    assertThat(result.getGasCost()).isEqualTo(2100L);
  }

  @Test
  void chargesAWarmSlotLess() {
    worldUpdater.getOrCreate(ADDRESS);
    final MessageFrame frame = frame();
    frame.warmUpStorage(ADDRESS, KEY);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(100L);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(org.hyperledger.besu.evm.UInt256.ZERO);
  }

  @Test
  void costsTheFrontierPriceBeforeBerlin() {
    worldUpdater.getOrCreate(ADDRESS);

    final Operation.OperationResult result =
        new SLoadOperationV2(new FrontierGasCalculator()).execute(frame(), null);

    assertThat(result.getGasCost()).isEqualTo(50L);
  }

  @Test
  void haltsWithoutGasForTheColdAccess() {
    worldUpdater.getOrCreate(ADDRESS);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .address(ADDRESS)
            .worldUpdater(worldUpdater)
            .initialGas(2099L)
            .pushStackItem(KEY)
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void haltsOnStackUnderflow() {
    worldUpdater.getOrCreate(ADDRESS);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().address(ADDRESS).worldUpdater(worldUpdater).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame frame() {
    return new TestMessageFrameBuilderV2()
        .address(ADDRESS)
        .worldUpdater(worldUpdater)
        .pushStackItem(KEY)
        .build();
  }
}
