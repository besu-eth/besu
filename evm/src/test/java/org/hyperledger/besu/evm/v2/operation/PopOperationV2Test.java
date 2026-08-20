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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.UnderflowException;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PopOperationV2Test {
  private MessageFrame frame;

  @BeforeEach
  public void setUp() {
    frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(new ToyWorld())
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(new ToyBlockValues())
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .initialGas(1)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .code(Code.EMPTY_CODE)
            .build();
  }

  @Test
  void popStackEmpty() {
    assertThrows(UnderflowException.class, () -> PopOperationV2.staticOperation(frame));
  }

  @Test
  void popStackSingleValue() {
    PushOperationV2.staticOperation(frame, new byte[] {0x00, 0x01}, 0, 1);
    PopOperationV2.staticOperation(frame);

    assertThat(frame.stackTopV2()).isEqualTo(0);
  }

  @Test
  void popStackValues() {
    PushOperationV2.staticOperation(frame, new byte[] {0x00, 0x01}, 0, 1);
    PushOperationV2.staticOperation(frame, new byte[] {0x00, 0x02}, 0, 1);
    PushOperationV2.staticOperation(frame, new byte[] {0x00, 0x03}, 0, 1);
    PopOperationV2.staticOperation(frame);

    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(frame.getStackItemV2(0)).isEqualTo(UInt256.fromInt(0x02));
    assertThat(frame.getStackItemV2(1)).isEqualTo(UInt256.fromInt(0x01));
  }
}
