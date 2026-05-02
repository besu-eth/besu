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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class ChainIdOperationTest {

  private final MessageFrame messageFrame = mock(MessageFrame.class);

  static Iterable<Arguments> params() {
    return List.of(
        Arguments.of("0x01", 2),
        Arguments.of("0x03", 2),
        Arguments.of("0x04", 2),
        Arguments.of("0x05", 2));
  }

  @ParameterizedTest
  @MethodSource("params")
  void shouldReturnChainId(final String chainIdString, final int expectedGas) {
    Bytes32 chainId = Bytes32.fromHexString(chainIdString);
    ChainIdOperation operation = new ChainIdOperation(new ConstantinopleGasCalculator(), chainId);
    final ArgumentCaptor<Bytes> arg = ArgumentCaptor.forClass(Bytes.class);
    when(messageFrame.getRemainingGas()).thenReturn(100L);
    operation.execute(messageFrame, null);
    Mockito.verify(messageFrame).getRemainingGas();
    Mockito.verify(messageFrame).pushStackItem(arg.capture());
    Mockito.verifyNoMoreInteractions(messageFrame);
    assertThat(arg.getValue()).isEqualTo(chainId);
  }

  @ParameterizedTest
  @MethodSource("params")
  void shouldCalculateGasPrice(final String chainIdString, final int expectedGas) {
    Bytes32 chainId = Bytes32.fromHexString(chainIdString);
    ChainIdOperation operation = new ChainIdOperation(new ConstantinopleGasCalculator(), chainId);
    final OperationResult result = operation.execute(messageFrame, null);
    assertThat(result.getGasCost()).isEqualTo(expectedGas);
  }
}
