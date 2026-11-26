/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessageFrameTest {

  private static final Bytes32 WORD1 = Bytes32.fromHexString(Long.toString(1).repeat(64));
  private static final Bytes32 WORD2 = Bytes32.fromHexString(Long.toString(2).repeat(64));

  private MessageFrame.Builder messageFrameBuilder;

  @BeforeEach
  void setUp() {
    messageFrameBuilder =
        MessageFrame.builder()
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
            .completer(messageFrame -> {});
  }

  @Test
  void shouldNotExpandMemory() {
    final MessageFrame messageFrame = messageFrameBuilder.build();

    final Bytes value = Bytes.concatenate(WORD1, WORD2);
    messageFrame.writeMemory(0, value.size(), value);
    int initialActiveWords = messageFrame.memoryWordSize();

    // Fully in bounds read
    assertThat(messageFrame.shadowReadMemory(64, Bytes32.SIZE)).isEqualTo(Bytes32.ZERO);
    assertThat(messageFrame.memoryWordSize()).isEqualTo(initialActiveWords);

    // Straddling read
    final Bytes straddlingRead = messageFrame.shadowReadMemory(50, Bytes32.SIZE);
    assertThat(messageFrame.memoryWordSize()).isEqualTo(initialActiveWords);
    assertThat(straddlingRead.get(0)).isEqualTo((byte) 0x22); // Still in WORD2
    assertThat(straddlingRead.get(13)).isEqualTo((byte) 0x22); // Just before uninitialized memory
    assertThat(straddlingRead.get(14)).isEqualTo((byte) 0); // Just in uninitialized memory
    assertThat(straddlingRead.get(20)).isEqualTo((byte) 0); // In uninitialized memory

    // Fully out of bounds read
    assertThat(messageFrame.shadowReadMemory(64, Bytes32.SIZE)).isEqualTo(Bytes32.ZERO);
    assertThat(messageFrame.memoryWordSize()).isEqualTo(initialActiveWords);

    assertThat(messageFrame.shadowReadMemory(32, Bytes32.SIZE)).isEqualTo(WORD2);
    assertThat(messageFrame.memoryWordSize()).isEqualTo(initialActiveWords);
  }
}
