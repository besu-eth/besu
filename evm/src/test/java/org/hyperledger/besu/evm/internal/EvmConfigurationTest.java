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
package org.hyperledger.besu.evm.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.evm.internal.EvmConfiguration.WorldUpdaterMode;

import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class EvmConfigurationTest {

  private static final EvmConfiguration V1 =
      new EvmConfiguration(32_000L, WorldUpdaterMode.STACKED, true, false);
  private static final EvmConfiguration V2 =
      new EvmConfiguration(32_000L, WorldUpdaterMode.STACKED, true, true);

  @Test
  void evmV2RejectsAnotherStackSize() {
    assertThatThrownBy(() -> withStackSize(V2, 2048))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("EVM v2 supports only a stack size of 1024");
  }

  @Test
  void evmV2AcceptsTheDefaultStackSize() {
    assertThat(withStackSize(V2, 1024).evmStackSize()).isEqualTo(1024);
  }

  @Test
  void evmV1AcceptsAnotherStackSize() {
    assertThat(withStackSize(V1, 2048).evmStackSize()).isEqualTo(2048);
  }

  private static EvmConfiguration withStackSize(
      final EvmConfiguration configuration, final int evmStackSize) {
    return configuration.overrides(
        OptionalInt.empty(), OptionalInt.empty(), OptionalInt.of(evmStackSize));
  }
}
