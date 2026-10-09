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
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ExtCodeSizeOperationV2Test {

  private static final Address ACCOUNT = Address.fromHexString("0xc0de");

  private final WorldUpdater worldUpdater = new ToyWorld().updater();

  @Test
  void pushesTheCodeSize() {
    worldUpdater.getOrCreate(ACCOUNT).setCode(Bytes.fromHexString("0x60016002016003"));
    final MessageFrame frame = frame(Bytes32.leftPad(ACCOUNT.getBytes()));

    final Operation.OperationResult result =
        new ExtCodeSizeOperationV2(new BerlinGasCalculator()).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromLong(7));
    assertThat(result.getGasCost()).isEqualTo(2600);
  }

  @Test
  void pushesZeroForAMissingAccount() {
    final MessageFrame frame = frame(Bytes32.leftPad(ACCOUNT.getBytes()));

    new ExtCodeSizeOperationV2(new BerlinGasCalculator()).execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void ignoresTheBitsAboveTheAddress() {
    worldUpdater.getOrCreate(ACCOUNT).setCode(Bytes.fromHexString("0x6001"));
    final MessageFrame frame =
        frame(
            Bytes32.fromHexString(
                "0xffffffffffffffffffffffff000000000000000000000000000000000000c0de"));

    new ExtCodeSizeOperationV2(new BerlinGasCalculator()).execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromLong(2));
  }

  @Test
  void chargesAWarmAccountLess() {
    final GasCalculator gasCalculator = new BerlinGasCalculator();
    final MessageFrame frame = frame(Bytes32.leftPad(ACCOUNT.getBytes()));
    frame.warmUpAddress(ACCOUNT);

    final Operation.OperationResult result =
        new ExtCodeSizeOperationV2(gasCalculator).execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getWarmStorageReadCost());
  }

  @Test
  void costsTheFrontierPriceBeforeBerlin() {
    final Operation.OperationResult result =
        new ExtCodeSizeOperationV2(new FrontierGasCalculator())
            .execute(frame(Bytes32.leftPad(ACCOUNT.getBytes())), null);

    assertThat(result.getGasCost()).isEqualTo(20);
  }

  @Test
  void haltsOnStackUnderflow() {
    final Operation.OperationResult result =
        new ExtCodeSizeOperationV2(new BerlinGasCalculator())
            .execute(new TestMessageFrameBuilderV2().build(), null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame frame(final Bytes32 address) {
    return new TestMessageFrameBuilderV2()
        .worldUpdater(worldUpdater)
        .initialGas(1_000_000)
        .pushStackItem(address)
        .build();
  }
}
