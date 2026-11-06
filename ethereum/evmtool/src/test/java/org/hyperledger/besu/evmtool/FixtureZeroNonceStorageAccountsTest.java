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
package org.hyperledger.besu.evmtool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.referencetests.BlockchainReferenceTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.EngineTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** The EIP-8253 account list block-test and engine-test read from a fixture's config. */
class FixtureZeroNonceStorageAccountsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Address FIRST =
      Address.fromHexString("0xf468bcbc4a0bfdb06336e773382c5202e674db71");
  private static final Address SECOND =
      Address.fromHexString("0xd8253352f6044cfe55bcc0748c3fa37b7df81f98");

  static Stream<Arguments> fixtureConfigs() {
    final Function<String, List<Address>> blockchain =
        FixtureZeroNonceStorageAccountsTest::blockchainAccounts;
    final Function<String, List<Address>> engine =
        FixtureZeroNonceStorageAccountsTest::engineAccounts;
    final String listed = "{\"zeroNonceStorageAccounts\": [\"" + FIRST + "\", \"" + SECOND + "\"]}";
    final String explicitNull = "{\"zeroNonceStorageAccounts\": null}";
    return Stream.of(
        Arguments.of("blockchain listed", blockchain, listed, List.of(FIRST, SECOND)),
        Arguments.of("blockchain absent", blockchain, "{}", List.of()),
        Arguments.of("blockchain null", blockchain, explicitNull, List.of()),
        Arguments.of("engine listed", engine, listed, List.of(FIRST, SECOND)),
        Arguments.of("engine absent", engine, "{}", List.of()),
        Arguments.of("engine null", engine, explicitNull, List.of()));
  }

  static Stream<Arguments> malformedConfigs() {
    final Function<String, List<Address>> blockchain =
        FixtureZeroNonceStorageAccountsTest::blockchainAccounts;
    final Function<String, List<Address>> engine =
        FixtureZeroNonceStorageAccountsTest::engineAccounts;
    final String notArray = "{\"zeroNonceStorageAccounts\": \"" + FIRST + "\"}";
    final String numberEntry = "{\"zeroNonceStorageAccounts\": [5]}";
    final String nullEntry = "{\"zeroNonceStorageAccounts\": [null]}";
    return Stream.of(
        Arguments.of("blockchain not an array", blockchain, notArray),
        Arguments.of("blockchain number entry", blockchain, numberEntry),
        Arguments.of("blockchain null entry", blockchain, nullEntry),
        Arguments.of("engine not an array", engine, notArray),
        Arguments.of("engine number entry", engine, numberEntry),
        Arguments.of("engine null entry", engine, nullEntry));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("fixtureConfigs")
  void readsTheAccountListInFixtureOrder(
      final String name,
      final Function<String, List<Address>> accountsOf,
      final String config,
      final List<Address> expected) {
    assertThat(accountsOf.apply(config)).containsExactlyElementsOf(expected);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("malformedConfigs")
  void rejectsAMalformedAccountList(
      final String name, final Function<String, List<Address>> accountsOf, final String config) {
    // block-test fails the fixture read, inside Jackson; engine-test fails the getter.
    assertThatThrownBy(() -> accountsOf.apply(config))
        .satisfies(
            thrown ->
                assertThat(Throwables.getRootCause(thrown))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("zeroNonceStorageAccounts"));
  }

  @Test
  void cachesSchedulesPerAccountList() {
    final EvmConfiguration evm = EvmConfiguration.DEFAULT;
    final ReferenceTestProtocolSchedules listed =
        ReferenceTestProtocolSchedules.cached(evm, null, List.of(FIRST));

    assertThat(ReferenceTestProtocolSchedules.cached(evm, null, List.of(FIRST))).isSameAs(listed);
    assertThat(ReferenceTestProtocolSchedules.cached(evm, null, List.of())).isNotSameAs(listed);
  }

  private static List<Address> blockchainAccounts(final String config) {
    return read(config, BlockchainReferenceTestCaseSpec.class).getZeroNonceStorageAccounts();
  }

  private static List<Address> engineAccounts(final String config) {
    return read(config, EngineTestCaseSpec.class).getZeroNonceStorageAccounts();
  }

  private static <T> T read(final String config, final Class<T> type) {
    try {
      return MAPPER.readValue(
          "{\"lastblockhash\": \"0x" + "00".repeat(32) + "\", \"config\": " + config + "}", type);
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
