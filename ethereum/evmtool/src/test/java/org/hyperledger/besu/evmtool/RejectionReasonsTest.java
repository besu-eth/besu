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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class RejectionReasonsTest {

  @Test
  void errorWithoutReasonsIsUnchanged() {
    assertThat(RejectionReasons.append("Header validation failed (LIGHT)", List.of()))
        .isEqualTo("Header validation failed (LIGHT)");
  }

  @Test
  void reasonsFollowTheErrorInBrackets() {
    assertThat(RejectionReasons.append("generic", List.of("first", "second", "first")))
        .isEqualTo("generic [first; second]");
  }

  @Test
  void reasonTheErrorAlreadyCarriesIsNotRepeated() {
    assertThat(RejectionReasons.append("Requests hash mismatch, x", List.of("Requests hash")))
        .isEqualTo("Requests hash mismatch, x");
  }

  @Test
  void onlyLinesLoggedWhileCapturingAreKept() {
    RejectionReasons.capture("outside any import");
    final List<String> reasons = new ArrayList<>();

    RejectionReasons.capturing(
        reasons,
        () -> {
          RejectionReasons.capture("Invalid block header: gas used 2 exceeds gas limit 1");
          RejectionReasons.capture("first line\nsecond line");
          RejectionReasons.capture("Invalid block RLP : 0xf9");
          RejectionReasons.capture("--- BAL constructed during execution ---\n...");
          return null;
        });

    assertThat(reasons)
        .containsExactly("Invalid block header: gas used 2 exceeds gas limit 1", "first line");
  }
}
