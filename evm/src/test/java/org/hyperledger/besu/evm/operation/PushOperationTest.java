/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.operation.PushOperation.staticOperation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PushOperationTest {

  private static final byte[] byteCode = new byte[] {0x00, 0x01, 0x02, 0x03};
  private static final MessageFrame frame =
      MessageFrame.builder()
          .enableEvmV2(false)
          .worldUpdater(new ToyWorld())
          .originator(Address.ZERO)
          .gasPrice(Wei.ONE)
          .blobGasPrice(Wei.ONE)
          .blockValues(new ToyBlockValues())
          .miningBeneficiary(Address.ZERO)
          .blockHashLookup((__, ___) -> Hash.ZERO)
          .type(MessageFrame.Type.MESSAGE_CALL)
          .initialGas(1)
          .address(Address.ZERO)
          .contract(Address.ZERO)
          .inputData(Bytes32.ZERO)
          .sender(Address.ZERO)
          .value(Wei.ZERO)
          .apparentValue(Wei.ZERO)
          .code(Code.EMPTY_CODE)
          .completer(messageFrame -> {})
          .build();
  ;

  @Test
  void unpaddedPushDoesntReachEndCode() {
    staticOperation(frame, byteCode, 0, byteCode.length - 2);
    assertThat(frame.getStackItem(0).equals(Bytes.fromHexString("0x0102"))).isTrue();
  }

  @Test
  void unpaddedPushUpReachesEndCode() {
    staticOperation(frame, byteCode, 0, byteCode.length - 1);
    assertThat(frame.getStackItem(0).equals(Bytes.fromHexString("0x010203"))).isTrue();
  }

  @Test
  void paddedPush() {
    staticOperation(frame, byteCode, 1, byteCode.length - 1);
    assertThat(frame.getStackItem(0).equals(Bytes.fromHexString("0x020300"))).isTrue();
  }

  @Test
  void oobPush() {
    staticOperation(frame, byteCode, byteCode.length, byteCode.length - 1);
    assertThat(frame.getStackItem(0).equals(Bytes.EMPTY)).isTrue();
  }
}
