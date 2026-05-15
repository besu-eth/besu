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
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class CoinbaseOperationV2Test {

  private final GasCalculator gasCalculator = new BerlinGasCalculator();
  private final CoinbaseOperationV2 operation = new CoinbaseOperationV2(gasCalculator);

  @Test
  void shouldPushCoinbaseToStack() {
    final Address coinbase = Address.fromHexString("0x1122334455667788990011223344556677889900");
    final MessageFrame frame = new TestMessageFrameBuilderV2().miningBeneficiary(coinbase).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    // The address is right-aligned in a 256-bit word; the top 96 bits are zero.
    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.leftPad(coinbase.getBytes()).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  @Test
  void shouldPushZeroAddressCoinbase() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().miningBeneficiary(Address.ZERO).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushAddressWithHighBytesSet() {
    final Address coinbase = Address.fromHexString("0xFFEEDDCCBBAA99887766554433221100AABBCCDD");
    final MessageFrame frame = new TestMessageFrameBuilderV2().miningBeneficiary(coinbase).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    final UInt256 stackVal = getV2StackItem(frame, 0);
    // u3 is always zero (upper 64 bits of the 256-bit word).
    assertThat(stackVal.u3()).isZero();
    // Upper 32 bits of u2 are zero (bits 65..96 of the 256-bit word).
    assertThat(stackVal.u2() >>> 32).isZero();
    // Reconstruct the address from the stack word and verify round-trip.
    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.leftPad(coinbase.getBytes()).toArrayUnsafe());
    assertThat(stackVal).isEqualTo(expected);
  }

  @Test
  void shouldReturnCorrectGasCost() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().miningBeneficiary(Address.ZERO).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getBaseTierGasCost());
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().initialGas(1L).miningBeneficiary(Address.ZERO).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().miningBeneficiary(Address.ZERO).build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }

  @Test
  void shouldHaltOnInsufficientGasEvenStackOverflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().miningBeneficiary(Address.ZERO).initialGas(1L).build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }
}
