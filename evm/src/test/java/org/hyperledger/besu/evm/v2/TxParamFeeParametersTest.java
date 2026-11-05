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
package org.hyperledger.besu.evm.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The EIP-8141 TXPARAM fee parameters on the v1 interpreter and on EVM v2: 0x03 is the max priority
 * fee per gas and 0x04 the fee budget, while 0x05 and 0x06, the devnet-1 max fee per blob gas and
 * max cost, are undefined.
 */
class TxParamFeeParametersTest {

  private static final long GAS = 100_000L;

  static Stream<Arguments> evms() {
    return Stream.of(Arguments.of("v1", ProgramRun.V1), Arguments.of("v2", ProgramRun.V2));
  }

  /** PUSH1 param, TXPARAM, STOP. */
  private static ProgramRun.Outcome txparam(final EvmConfiguration configuration, final int param) {
    return ProgramRun.executeTracedInFrameTransaction(
            MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, configuration),
            Bytes.of(0x60, param, 0xb0, 0x00),
            Bytes.EMPTY,
            GAS,
            false,
            List.of(Bytes32.ZERO),
            AssertContexts.withoutDiff(true))
        .outcome();
  }

  private static String word(final Bytes value) {
    return Bytes32.leftPad(value).toUnprefixedHexString();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("evms")
  void readsThePriorityFeeAndTheFeeBudget(
      final String evmName, final EvmConfiguration configuration) {
    assertThat(txparam(configuration, 0x03).stack())
        .containsExactly(word(AssertContexts.MAX_PRIORITY_FEE_PER_GAS.toBytes()));
    assertThat(txparam(configuration, 0x04).stack())
        .containsExactly(word(AssertContexts.MAX_FEE.toBytes()));
  }

  static Stream<Arguments> evmsAndRemovedParameters() {
    return evms()
        .flatMap(
            evm ->
                Stream.of(0x05, 0x06)
                    .map(param -> Arguments.of(evm.get()[0], evm.get()[1], param)));
  }

  @ParameterizedTest(name = "{0}, TXPARAM({2})")
  @MethodSource("evmsAndRemovedParameters")
  void haltsOnTheRemovedBlobFeeAndMaxCostParameters(
      final String evmName, final EvmConfiguration configuration, final int param) {
    final ProgramRun.Outcome outcome = txparam(configuration, param);
    assertThat(outcome.haltReason()).isEqualTo("INVALID_OPERATION");
    assertThat(outcome.remainingGas()).isZero();
  }
}
