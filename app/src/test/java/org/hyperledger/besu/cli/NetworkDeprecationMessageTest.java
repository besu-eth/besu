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
package org.hyperledger.besu.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.config.NetworkDefinition;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NetworkDeprecationMessageTest {

  @ParameterizedTest
  @EnumSource(
      value = NetworkDefinition.class,
      names = {"HOLESKY"})
  void shouldGenerateDeprecationMessageForDeprecatedNetworks(final NetworkDefinition network) {
    assertThat(NetworkDeprecationMessage.generate(network))
        .contains(network.normalize() + " is deprecated");
  }

  @ParameterizedTest
  @EnumSource(
      value = NetworkDefinition.class,
      names = {"MAINNET", "SEPOLIA", "DEV", "LUKSO", "EPHEMERY", "HOODI"})
  void shouldThrowErrorForNonDeprecatedNetworks(final NetworkDefinition network) {
    assertThatThrownBy(() -> NetworkDeprecationMessage.generate(network))
        .isInstanceOf(AssertionError.class);
  }
}
