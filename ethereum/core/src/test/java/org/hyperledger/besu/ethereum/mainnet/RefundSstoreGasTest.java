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
package org.hyperledger.besu.ethereum.mainnet;

import static org.apache.tuweni.units.bigints.UInt256.ONE;
import static org.apache.tuweni.units.bigints.UInt256.ZERO;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.IstanbulGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RefundSstoreGasTest {

  private static final UInt256 TWO = UInt256.valueOf(2);

  public static Stream<Arguments> scenarios() {
    final GasCalculator constantinople = new ConstantinopleGasCalculator();
    final GasCalculator petersburg = new PetersburgGasCalculator();
    final GasCalculator istanbul = new IstanbulGasCalculator();
    return Stream.of(
        // Zero no-op
        Arguments.of("constantinople", constantinople, ZERO, ZERO, ZERO, 200L, 0L),
        Arguments.of("petersburg", petersburg, ZERO, ZERO, ZERO, 5_000L, 0L),
        Arguments.of("istanbul", istanbul, ZERO, ZERO, ZERO, 800L, 0L),
        // Zero fresh change
        Arguments.of("constantinople", constantinople, ZERO, ZERO, ONE, 20_000L, 0L),
        Arguments.of("petersburg", petersburg, ZERO, ZERO, ONE, 20_000L, 0L),
        Arguments.of("istanbul", istanbul, ZERO, ZERO, ONE, 20_000L, 0L),
        // Dirty, reset to zero
        Arguments.of("constantinople", constantinople, ZERO, ONE, ZERO, 200L, 19_800L),
        Arguments.of("petersburg", petersburg, ZERO, ONE, ZERO, 5_000L, 15_000L),
        Arguments.of("istanbul", istanbul, ZERO, ONE, ZERO, 800L, 19_200L),
        // Dirty, changed but not reset
        Arguments.of("constantinople", constantinople, ZERO, ONE, TWO, 200L, 0L),
        Arguments.of("petersburg", petersburg, ZERO, ONE, TWO, 5_000L, 0L),
        Arguments.of("istanbul", istanbul, ZERO, ONE, TWO, 800L, 0L),
        // Dirty no-op
        Arguments.of("constantinople", constantinople, ZERO, ONE, ONE, 200L, 0L),
        Arguments.of("petersburg", petersburg, ZERO, ONE, ONE, 5_000L, 0L),
        Arguments.of("istanbul", istanbul, ZERO, ONE, ONE, 800L, 0L),
        // Dirty, zero no-op
        Arguments.of("constantinople", constantinople, ONE, ZERO, ZERO, 200L, 0L),
        Arguments.of("petersburg", petersburg, ONE, ZERO, ZERO, 5_000L, 0L),
        Arguments.of("istanbul", istanbul, ONE, ZERO, ZERO, 800L, 0L),
        // Dirty, reset to non-zero
        Arguments.of("constantinople", constantinople, ONE, ZERO, ONE, 200L, -15_000L + 4_800L),
        Arguments.of("petersburg", petersburg, ONE, ZERO, ONE, 20_000L, 0L),
        Arguments.of("istanbul", istanbul, ONE, ZERO, ONE, 800L, -15_000L + 4_200L),
        // Fresh change to zero
        Arguments.of("constantinople", constantinople, ONE, ONE, ZERO, 5_000L, 15_000L),
        Arguments.of("petersburg", petersburg, ONE, ONE, ZERO, 5_000L, 15_000L),
        Arguments.of("istanbul", istanbul, ONE, ONE, ZERO, 5_000L, 15_000L),
        // Fresh change with all non-zero
        Arguments.of("constantinople", constantinople, ONE, ONE, TWO, 5_000L, 0L),
        Arguments.of("petersburg", petersburg, ONE, ONE, TWO, 5_000L, 0L),
        Arguments.of("istanbul", istanbul, ONE, ONE, TWO, 5_000L, 0L),
        // Dirty, clear originally set value
        Arguments.of("constantinople", constantinople, ONE, TWO, ZERO, 200L, 15_000L),
        Arguments.of("petersburg", petersburg, ONE, TWO, ZERO, 5_000L, 15_000L),
        Arguments.of("istanbul", istanbul, ONE, TWO, ZERO, 800L, 15_000L),
        // Non-zero no-op
        Arguments.of("constantinople", constantinople, ONE, ONE, ONE, 200L, 0L),
        Arguments.of("petersburg", petersburg, ONE, ONE, ONE, 5_000L, 0L),
        Arguments.of("istanbul", istanbul, ONE, ONE, ONE, 800L, 0L));
  }

  private final Supplier<UInt256> mockSupplierForOriginalValue = mockSupplier();
  private final Supplier<UInt256> mockSupplierCurrentValue = mockSupplier();

  @SuppressWarnings("unchecked")
  private <T> Supplier<T> mockSupplier() {
    return mock(Supplier.class);
  }

  public void setUp(final UInt256 originalValue, final UInt256 currentValue) {
    when(mockSupplierForOriginalValue.get()).thenReturn(originalValue);
    when(mockSupplierCurrentValue.get()).thenReturn(currentValue);
  }

  @ParameterizedTest(name = "calculator: {0}, original: {2}, current: {3}, new: {4}")
  @MethodSource("scenarios")
  public void shouldChargeCorrectGas(
      final String forkName,
      final GasCalculator gasCalculator,
      final UInt256 originalValue,
      final UInt256 currentValue,
      final UInt256 newValue,
      final long expectedGasCost,
      final long expectedGasRefund) {
    setUp(originalValue, currentValue);
    Assertions.assertThat(
            gasCalculator.calculateStorageCost(
                newValue, mockSupplierCurrentValue, mockSupplierForOriginalValue))
        .isEqualTo(expectedGasCost);
  }

  @ParameterizedTest(name = "calculator: {0}, original: {2}, current: {3}, new: {4}")
  @MethodSource("scenarios")
  public void shouldRefundCorrectGas(
      final String forkName,
      final GasCalculator gasCalculator,
      final UInt256 originalValue,
      final UInt256 currentValue,
      final UInt256 newValue,
      final long expectedGasCost,
      final long expectedGasRefund) {
    setUp(originalValue, currentValue);
    Assertions.assertThat(
            gasCalculator.calculateStorageRefundAmount(
                newValue, mockSupplierCurrentValue, mockSupplierForOriginalValue))
        .isEqualTo(expectedGasRefund);
  }
}
