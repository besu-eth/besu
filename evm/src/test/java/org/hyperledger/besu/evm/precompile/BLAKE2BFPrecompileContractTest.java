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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.crypto.Blake2bfMessageDigest.Blake2bfDigest;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.frame.MessageFrame;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BLAKE2BFPrecompileContractTest {
  private final PrecompiledContract contract =
      EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL)
          .getPrecompileContractRegistry()
          .get(Address.BLAKE2B_F_COMPRESSION);

  BLAKE2BFPrecompileContractTest() {}

  private final MessageFrame messageFrame = mock(MessageFrame.class);

  static Arguments[] parameters() {
    return new Arguments[] {
      Arguments.of( // https://tools.ietf.org/html/rfc7693#appendix-A
          "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000001",
          "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923",
          12),
      Arguments.of( // https://tools.ietf.org/html/rfc7693#appendix-A, not final
          "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000000",
          "75ab69d3190a562c51aef8d88f1c2775876944407270c42c9844252c26d2875298743e7f6d5ea2f2d3e8d226039cd31b4e426ac4f2d3d666a610c2116fde4735",
          12),
      Arguments.of( // as above, but 8 million rounds.
          "007a120048c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000001",
          "6d2ce9e534d50e18ff866ae92d70cceba79bbcd14c63819fe48752c8aca87a4bb7dcc230d22a4047f0486cfcfb50a17b24b2899eb8fca370f22240adb5170189",
          8_000_000),
      Arguments.of( // empty
          "", null, 0),
      Arguments.of( // too long
          "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b6162630000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000030000000000000000000000000000000001",
          null,
          0),
      Arguments.of( // too short
          "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b616263000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000001",
          null,
          0),
      Arguments.of( // bad final
          "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000011",
          null,
          0),
    };
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void shouldRunFCompressionNative(
      final String inputString, final String expectedResult, final long expectedGasUsed) {
    Blake2bfDigest.maybeEnableNative();

    testFCompression(inputString, expectedResult, expectedGasUsed);
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void shouldRunFCompressionJava(
      final String inputString, final String expectedResult, final long expectedGasUsed) {
    Blake2bfDigest.disableNative();

    testFCompression(inputString, expectedResult, expectedGasUsed);
  }

  private void testFCompression(
      final String inputString, final String expectedResult, final long expectedGasUsed) {
    final Bytes input = Bytes.fromHexString(inputString);
    final Bytes expectedComputation =
        expectedResult == null ? null : Bytes.fromHexString(expectedResult);
    assertThat(contract.computePrecompile(input, messageFrame).output())
        .isEqualTo(expectedComputation);
    assertThat(contract.gasRequirement(input)).isEqualTo(expectedGasUsed);
  }
}
