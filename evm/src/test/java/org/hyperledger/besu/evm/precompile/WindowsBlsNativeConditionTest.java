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
package org.hyperledger.besu.evm.precompile;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class WindowsBlsNativeConditionTest {

  @ParameterizedTest
  @CsvSource({
    "true, false, true",
    "true, true, false",
    "false, false, false",
    "false, true, false"
  })
  void skipsOnlyWindowsWithoutGnark(
      final boolean windows, final boolean nativeAvailable, final boolean disabled) {
    final AtomicBoolean probed = new AtomicBoolean();
    final var result =
        WindowsBlsNativeCondition.evaluate(
            windows,
            () -> {
              probed.set(true);
              return nativeAvailable;
            });

    assertThat(result.isDisabled()).isEqualTo(disabled);
    assertThat(probed.get()).isEqualTo(windows);
  }
}
