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
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ExtCodeHashOperationV2Test {

  private static final Address ACCOUNT = Address.fromHexString("0xc0de");

  private final WorldUpdater worldUpdater = new ToyWorld().updater();
  private final ExtCodeHashOperationV2 operation =
      new ExtCodeHashOperationV2(new BerlinGasCalculator());

  @Test
  void pushesTheCodeHash() {
    final Bytes code = Bytes.fromHexString("0x60016002016003");
    worldUpdater.getOrCreate(ACCOUNT).setCode(code);
    final MessageFrame frame = frame();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(Hash.hash(code).getBytes().toArrayUnsafe()));
    assertThat(result.getGasCost()).isEqualTo(2600);
  }

  @Test
  void pushesTheEmptyCodeHashForAnAccountWithoutCode() {
    worldUpdater.getOrCreate(ACCOUNT).setBalance(Wei.ONE);
    final MessageFrame frame = frame();

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(Hash.EMPTY.getBytes().toArrayUnsafe()));
  }

  @Test
  void pushesZeroForAnEmptyAccount() {
    worldUpdater.getOrCreate(ACCOUNT);
    final MessageFrame frame = frame();

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void pushesZeroForAMissingAccount() {
    final MessageFrame frame = frame();

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void costsTheConstantinoplePriceBeforeBerlin() {
    final Operation.OperationResult result =
        new ExtCodeHashOperationV2(new ConstantinopleGasCalculator()).execute(frame(), null);

    assertThat(result.getGasCost()).isEqualTo(400);
  }

  @Test
  void haltsOnStackUnderflow() {
    final Operation.OperationResult result =
        operation.execute(new TestMessageFrameBuilderV2().build(), null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame frame() {
    return new TestMessageFrameBuilderV2()
        .worldUpdater(worldUpdater)
        .initialGas(1_000_000)
        .pushStackItem(Bytes32.leftPad(ACCOUNT.getBytes()))
        .build();
  }
}
