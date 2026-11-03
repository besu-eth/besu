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
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.junit.jupiter.api.Test;

class AddressOperationV2Test extends NullaryOperationV2Test {

  public AddressOperationV2Test() {
    super(new AddressOperationV2(new FrontierGasCalculator()));
  }

  @Test
  void pushesTheRecipientAddress() {
    final MessageFrame frame = frame();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(new UInt256(0, 0x01234567L, 0x89abcdef01234567L, 0x89abcdef01234567L));
  }

  @Test
  void gasCostIsBaseTier() {
    final Operation.OperationResult result = operation.execute(frame(), null);

    assertThat(result.getGasCost()).isEqualTo(new FrontierGasCalculator().getBaseTierGasCost());
  }

  private static MessageFrame frame() {
    return new TestMessageFrameBuilderV2()
        .address(Address.fromHexString("0x0123456789abcdef0123456789abcdef01234567"))
        .build();
  }
}
