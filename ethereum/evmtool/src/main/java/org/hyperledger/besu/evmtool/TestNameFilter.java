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

import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * The test-id filter used by {@code block-test}, {@code state-test} and {@code engine-test}, in two
 * flavours.
 *
 * <p>{@code --test-name} ({@link #compile}) is the convenience form: an expression without {@code
 * *} or {@code ?} is a case-insensitive substring match, and one with either is a case-insensitive
 * pattern that must match the whole test id, where {@code *} becomes {@code .*}, {@code ?} becomes
 * {@code .} and a literal {@code .} is escaped — test ids are pytest node ids, which contain {@code
 * ".py"}, so a bare dot is far more often meant literally than as a wildcard.
 *
 * <p>{@code --test-name-regex} ({@link #compileRegex}) is the fidelity form: the expression is a
 * regex, passed to {@link Pattern} with nothing rewritten and nothing escaped. It reproduces hive's
 * {@code --sim.limit}, which the EELS simulators apply with Python's {@code re.match} — anchored at
 * the start of the node id, open at the end, case-sensitive. A hive filter can therefore be handed
 * over exactly as published, escapes and all.
 *
 * <p>Callers compile the expression before reading any fixture, so a malformed pattern fails up
 * front rather than part-way through a run.
 */
final class TestNameFilter {

  private final Pattern regex;
  private final String substring;
  private final boolean wholeIdMatch;

  private TestNameFilter(final Pattern regex, final String substring, final boolean wholeIdMatch) {
    this.regex = regex;
    this.substring = substring;
    this.wholeIdMatch = wholeIdMatch;
  }

  /**
   * Compiles whichever of the two filter options was given.
   *
   * @param testName the {@code --test-name} expression, or null
   * @param testNameRegex the {@code --test-name-regex} expression, or null
   * @return the compiled filter, or null when neither option was given
   * @throws IllegalArgumentException if both are given, or the expression is not a valid pattern
   */
  static TestNameFilter fromOptions(final String testName, final String testNameRegex) {
    if (testName != null && testNameRegex != null) {
      throw new IllegalArgumentException(
          "--test-name and --test-name-regex are mutually exclusive: --test-name rewrites '*' and"
              + " '?' and escapes '.', --test-name-regex takes the expression verbatim.");
    }
    if (testNameRegex != null) {
      return compileRegex(testNameRegex);
    }
    return testName == null ? null : compile(testName);
  }

  /**
   * Describes the active filter for the "nothing ran" message, so an empty run names the expression
   * that selected nothing.
   *
   * @param testName the {@code --test-name} expression, or null
   * @param testNameRegex the {@code --test-name-regex} expression, or null
   * @return a phrase to append to the message, empty when no filter was given
   */
  static String describe(final String testName, final String testNameRegex) {
    if (testNameRegex != null) {
      return " matching --test-name-regex '" + testNameRegex + "'";
    }
    return testName == null ? "" : " matching --test-name '" + testName + "'";
  }

  /**
   * Compiles a {@code --test-name} expression.
   *
   * @param expression the {@code --test-name} expression, never null
   * @return the compiled filter
   * @throws IllegalArgumentException if the expression is not a valid pattern
   */
  static TestNameFilter compile(final String expression) {
    if (expression.indexOf('*') < 0 && expression.indexOf('?') < 0) {
      return new TestNameFilter(null, expression.toLowerCase(Locale.ROOT), false);
    }
    // '.' is escaped: test ids are pytest node ids, which contain ".py"
    final String pattern = expression.replace(".", "\\.").replace("*", ".*").replace("?", ".");
    return new TestNameFilter(
        compilePattern(expression, pattern, Pattern.CASE_INSENSITIVE, "--test-name"), null, true);
  }

  /**
   * Compiles a {@code --test-name-regex} expression, i.e. a hive {@code --sim.limit} value.
   *
   * @param expression the regex, never null, taken verbatim
   * @return the compiled filter
   * @throws IllegalArgumentException if the expression is not a valid regex
   */
  static TestNameFilter compileRegex(final String expression) {
    return new TestNameFilter(
        compilePattern(expression, expression, 0, "--test-name-regex"), null, false);
  }

  private static Pattern compilePattern(
      final String expression, final String pattern, final int flags, final String option) {
    try {
      return Pattern.compile(pattern, flags);
    } catch (final PatternSyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s pattern '%s': %s."
                  + " Test ids contain regex metacharacters such as '[' and '('."
                  + " Escape them (\\[) to match literally.",
              option, expression, e.getDescription()),
          e);
    }
  }

  /**
   * Whether the given test id matches this filter.
   *
   * @param test the test id
   * @return true when it matches
   */
  boolean matches(final String test) {
    if (regex == null) {
      return test.toLowerCase(Locale.ROOT).contains(substring);
    }
    // lookingAt(), not matches(), for the regex form: Python's re.match anchors at the start only.
    return wholeIdMatch ? regex.matcher(test).matches() : regex.matcher(test).lookingAt();
  }
}
