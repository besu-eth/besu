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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class ProgressFormatTest {

  @Test
  void separatesThousands() {
    assertThat(ProgressFormat.count(0)).isEqualTo("0");
    assertThat(ProgressFormat.count(11_901_107)).isEqualTo("11,901,107");
    assertThat(ProgressFormat.block(15_537_394)).isEqualTo("#15,537,394");
  }

  @Test
  void compactsLargeCounts() {
    assertThat(ProgressFormat.compact(0)).isEqualTo("0");
    assertThat(ProgressFormat.compact(2_140)).isEqualTo("2,140");
    assertThat(ProgressFormat.compact(82_933)).isEqualTo("82.9K");
    assertThat(ProgressFormat.compact(1_312_471)).isEqualTo("1.31M");
    assertThat(ProgressFormat.compact(82_933_000)).isEqualTo("82.9M");
    assertThat(ProgressFormat.compact(1_200_000_000)).isEqualTo("1.20B");
    assertThat(ProgressFormat.compact(Long.MAX_VALUE)).isEqualTo("9223372036.85B");
  }

  @Test
  void picksTheUnitAfterRounding() {
    assertThat(ProgressFormat.compact(999_949)).isEqualTo("999.9K");
    assertThat(ProgressFormat.compact(999_950)).isEqualTo("1.00M");
    assertThat(ProgressFormat.compact(9_994_999)).isEqualTo("9.99M");
    assertThat(ProgressFormat.compact(9_995_000)).isEqualTo("10.0M");
    assertThat(ProgressFormat.compact(999_949_999)).isEqualTo("999.9M");
    assertThat(ProgressFormat.compact(999_950_000)).isEqualTo("1.00B");
  }

  @Test
  void writesDurationsInTwoUnitsAtMost() {
    assertThat(ProgressFormat.duration(Duration.ZERO)).isEqualTo("0s");
    assertThat(ProgressFormat.duration(Duration.ofSeconds(-5))).isEqualTo("0s");
    assertThat(ProgressFormat.duration(Duration.ofSeconds(45))).isEqualTo("45s");
    assertThat(ProgressFormat.duration(Duration.ofSeconds(190))).isEqualTo("3m 10s");
    assertThat(ProgressFormat.duration(Duration.ofMinutes(45))).isEqualTo("45m");
    assertThat(ProgressFormat.duration(Duration.ofMinutes(151))).isEqualTo("2h 31m");
    assertThat(ProgressFormat.duration(Duration.ofHours(26))).isEqualTo("1d 2h");
    assertThat(ProgressFormat.duration(Duration.ofDays(400))).isEqualTo("400d");
  }
}
