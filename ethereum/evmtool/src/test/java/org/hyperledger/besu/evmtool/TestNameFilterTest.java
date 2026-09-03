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

import org.junit.jupiter.api.Test;

class TestNameFilterTest {

  private static final String NODE_ID =
      "tests/amsterdam/eip2780_reduce_intrinsic_tx_gas/test_fork_transition.py"
          + "::test_intrinsic_reduction[fork_BPO2ToAmsterdamAtTime15k-non-zero_value-plain_call]";

  @Test
  void substringMatchIsCaseInsensitive() {
    assertThat(TestNameFilter.compile("FORK_bpo2toamsterdam").matches(NODE_ID)).isTrue();
    assertThat(TestNameFilter.compile("fork_Osaka").matches(NODE_ID)).isFalse();
  }

  @Test
  void alternationWorks() {
    // The hive --sim.limit equivalent, as documented in REFERENCE_TESTS.md.
    assertThat(
            TestNameFilter.compile("*fork_(Amsterdam|BPO2ToAmsterdamAtTime15k|Osaka)*")
                .matches(NODE_ID))
        .isTrue();
    assertThat(TestNameFilter.compile("*fork_(Cancun|Prague)*").matches(NODE_ID)).isFalse();
  }

  @Test
  void dotIsALiteralNotAWildcard() {
    assertThat(TestNameFilter.compile("*test_fork_transition.py*").matches(NODE_ID)).isTrue();
    assertThat(TestNameFilter.compile("*test_fork_transitionXpy*").matches(NODE_ID)).isFalse();
  }

  @Test
  void questionMarkMatchesASingleCharacter() {
    assertThat(TestNameFilter.compile("*plain_cal?]").matches(NODE_ID)).isTrue();
    assertThat(TestNameFilter.compile("*plain_ca?]").matches(NODE_ID)).isFalse();
  }

  @Test
  void patternMustMatchTheWholeName() {
    assertThat(TestNameFilter.compile("tests/amsterdam*").matches(NODE_ID)).isTrue();
    assertThat(TestNameFilter.compile("eip2780*").matches(NODE_ID)).isFalse();
  }

  @Test
  void malformedPatternIsRejectedWhenCompiled() {
    assertThatThrownBy(() -> TestNameFilter.compile("*[fork_Amsterdam*"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid --test-name pattern '*[fork_Amsterdam*'")
        .hasMessageContaining("Escape them");
  }

  @Test
  void metacharactersCanBeEscapedToMatchLiterally() {
    assertThat(TestNameFilter.compile("*\\[fork_BPO2ToAmsterdamAtTime15k*").matches(NODE_ID))
        .isTrue();
  }

  @Test
  void regexTakesAHiveSimLimitVerbatim() {
    // The published glamsterdam run's --sim.limit, copied character for character.
    assertThat(
            TestNameFilter.compileRegex(".*fork_(Amsterdam|BPO2ToAmsterdamAtTime15k|Osaka).*")
                .matches(NODE_ID))
        .isTrue();
    assertThat(TestNameFilter.compileRegex(".*(7708|7928|8282).*").matches(NODE_ID)).isFalse();
    assertThat(TestNameFilter.compileRegex(".*(2780|7928).*").matches(NODE_ID)).isTrue();
  }

  @Test
  void regexDoesNotEscapeTheDot() {
    // The case --test-name cannot express: an escaped dot stays an escaped dot, and a bare dot
    // stays a wildcard. Under --test-name both are rewritten and select nothing.
    assertThat(TestNameFilter.compileRegex(".*test_fork_transition\\.py.*").matches(NODE_ID))
        .isTrue();
    assertThat(TestNameFilter.compileRegex(".*test_fork_transition.py.*").matches(NODE_ID))
        .isTrue();
  }

  @Test
  void regexIsAnchoredAtTheStartAndOpenAtTheEnd() {
    // Python's re.match, which is how the EELS simulators apply --sim.limit.
    assertThat(TestNameFilter.compileRegex("tests/amsterdam").matches(NODE_ID)).isTrue();
    assertThat(TestNameFilter.compileRegex("eip2780").matches(NODE_ID)).isFalse();
  }

  @Test
  void regexIsCaseSensitive() {
    assertThat(TestNameFilter.compileRegex(".*fork_BPO2ToAmsterdam.*").matches(NODE_ID)).isTrue();
    assertThat(TestNameFilter.compileRegex(".*FORK_BPO2TOAMSTERDAM.*").matches(NODE_ID)).isFalse();
  }

  @Test
  void malformedRegexIsRejectedWhenCompiled() {
    assertThatThrownBy(() -> TestNameFilter.compileRegex(".*[fork_Amsterdam.*"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid --test-name-regex pattern");
  }

  @Test
  void fromOptionsPicksTheGivenOptionAndRejectsBoth() {
    assertThat(TestNameFilter.fromOptions(null, null)).isNull();
    assertThat(TestNameFilter.fromOptions("fork_BPO2ToAmsterdam", null).matches(NODE_ID)).isTrue();
    assertThat(TestNameFilter.fromOptions(null, ".*fork_BPO2ToAmsterdam.*").matches(NODE_ID))
        .isTrue();
    assertThatThrownBy(() -> TestNameFilter.fromOptions("a", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mutually exclusive");
  }
}
