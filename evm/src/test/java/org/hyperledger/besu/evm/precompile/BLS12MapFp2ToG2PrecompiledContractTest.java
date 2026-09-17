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
package org.hyperledger.besu.evm.precompile;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.evm.frame.MessageFrame;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.io.CharStreams;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

class BLS12MapFp2ToG2PrecompiledContractTest {

  final BLS12MapFp2ToG2PrecompiledContract contract = new BLS12MapFp2ToG2PrecompiledContract();

  private final MessageFrame messageFrame = mock(MessageFrame.class);

  static Iterable<Arguments> parameters() throws IOException {
    return CharStreams.readLines(
            new InputStreamReader(
                Objects.requireNonNull(
                    BLS12MapFp2ToG2PrecompiledContractTest.class.getResourceAsStream(
                        "fp2_to_g2.csv")),
                UTF_8))
        .stream()
        .map(line -> Arguments.of((Object[]) line.split(",", 4)))
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void shouldCalculate(
      final String inputString,
      final String expectedResult,
      final String expectedGasUsed,
      final String notes) {

    if ("input".equals(inputString)) {
      // skip the header row
      return;
    }
    final Bytes input = Bytes.fromHexString(inputString);
    final Bytes expectedComputation =
        expectedResult == null ? null : Bytes.fromHexString(expectedResult);
    final PrecompiledContract.PrecompileContractResult result =
        contract.computePrecompile(input, messageFrame);
    Bytes actualComputation = result.output();
    if (actualComputation == null) {
      final ArgumentCaptor<Bytes> revertReason = ArgumentCaptor.forClass(Bytes.class);
      verify(messageFrame).setRevertReason(revertReason.capture());
      assertThat(new String(revertReason.getValue().toArrayUnsafe(), UTF_8)).contains(notes);

      assertThat(expectedComputation.size()).isZero();
    } else {
      assertThat(actualComputation).isEqualTo(expectedComputation);
      assertThat(contract.gasRequirement(input)).isEqualTo(Long.parseLong(expectedGasUsed));
    }
  }
}
