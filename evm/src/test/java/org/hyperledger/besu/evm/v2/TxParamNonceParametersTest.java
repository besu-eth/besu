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
import static org.hyperledger.besu.evm.v2.ProgramRun.CONTRACT;
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.FrameInfo;
import org.hyperledger.besu.evm.frame.KeyedNonces;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The EIP-8250 TXPARAM nonce key parameters on the v1 interpreter and on EVM v2: 0x0E is the key
 * count, 0x0F the key set commitment, and 0x10 the first key, or zero for the empty set.
 */
class TxParamNonceParametersTest {

  private static final long GAS = 100_000L;

  static Stream<Arguments> evmsAndKeySets() {
    final List<List<UInt256>> keySets =
        List.of(
            KeyedNonces.LEGACY_NONCE_KEYS,
            List.of(UInt256.ZERO),
            List.of(UInt256.valueOf(5), UInt256.valueOf(7)));
    return Stream.of(Arguments.of("v1", ProgramRun.V1), Arguments.of("v2", ProgramRun.V2))
        .flatMap(
            evm -> keySets.stream().map(keys -> Arguments.of(evm.get()[0], evm.get()[1], keys)));
  }

  @ParameterizedTest(name = "{0}, nonce keys {2}")
  @MethodSource("evmsAndKeySets")
  void readsTheNonceKeyParameters(
      final String evmName, final EvmConfiguration configuration, final List<UInt256> nonceKeys) {
    final Bytes32 firstKey = nonceKeys.isEmpty() ? Bytes32.ZERO : nonceKeys.get(0);
    assertThat(txparam(configuration, nonceKeys, 0x0E).stack())
        .containsExactly(word(UInt256.valueOf(nonceKeys.size())));
    assertThat(txparam(configuration, nonceKeys, 0x0F).stack())
        .containsExactly(word(KeyedNonces.nonceKeysHash(nonceKeys)));
    assertThat(txparam(configuration, nonceKeys, 0x10).stack()).containsExactly(word(firstKey));
  }

  /** PUSH1 param, TXPARAM, STOP. */
  private static ProgramRun.Outcome txparam(
      final EvmConfiguration configuration, final List<UInt256> nonceKeys, final int param) {
    final ProgramRun.Outcome outcome =
        ProgramRun.executeTracedInFrameTransaction(
                MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, configuration),
                Bytes.of(0x60, param, 0xb0, 0x00),
                Bytes.EMPTY,
                GAS,
                false,
                List.of(Bytes32.ZERO),
                context(nonceKeys))
            .outcome();
    assertThat(outcome.haltReason()).isEmpty();
    return outcome;
  }

  /** A single SENDER frame targeting the contract, which is executing. */
  private static FrameTransactionContext context(final List<UInt256> nonceKeys) {
    return new FrameTransactionContext(
        List.of(
            new FrameInfo(
                FrameTransactionContext.MODE_SENDER,
                0,
                CONTRACT,
                50_000L,
                0L,
                Wei.ZERO,
                Bytes.EMPTY)),
        List.of(),
        SENDER,
        Bytes32.ZERO,
        3L,
        nonceKeys,
        3L,
        Wei.ONE,
        Wei.of(1_000),
        Wei.of(1_000));
  }

  private static String word(final Bytes value) {
    return Bytes32.leftPad(value).toUnprefixedHexString();
  }
}
