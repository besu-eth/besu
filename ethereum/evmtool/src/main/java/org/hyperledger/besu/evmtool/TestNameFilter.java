/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.evmtool;

import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * The {@code --run}/{@code --test-name} filter shared by the fixture runners: a case-insensitive
 * substring match, or — when the expression contains {@code *} or {@code ?} — a case-insensitive
 * regex in which {@code *} means {@code .*} and {@code ?} means any single character. Everything
 * else reaches {@link Pattern} untouched, so alternation and character classes work, mirroring
 * hive's {@code --sim.limit}.
 *
 * <p>Compiling once, before any fixture is read, is what makes a malformed expression an immediate
 * failure rather than one raised part-way through a run, where callers would have to distinguish it
 * from an unreadable fixture file.
 */
final class TestNameFilter {

  private final Pattern regex;
  private final String substring;

  private TestNameFilter(final Pattern regex, final String substring) {
    this.regex = regex;
    this.substring = substring;
  }

  /**
   * Compiles a filter expression.
   *
   * @param expression the {@code --run} expression, never null
   * @return the compiled filter
   * @throws IllegalArgumentException if the expression is not a valid pattern
   */
  static TestNameFilter compile(final String expression) {
    if (expression.indexOf('*') < 0 && expression.indexOf('?') < 0) {
      return new TestNameFilter(null, expression.toLowerCase(Locale.ROOT));
    }
    // '.' is a literal: test ids are pytest node ids, so they are full of ".py"
    final String pattern = expression.replace(".", "\\.").replace("*", ".*").replace("?", ".");
    try {
      return new TestNameFilter(Pattern.compile(pattern, Pattern.CASE_INSENSITIVE), null);
    } catch (final PatternSyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid --run/--test-name pattern '%s': %s."
                  + " Test ids contain regex metacharacters such as '[' and '(' — escape them"
                  + " (\\[) if you mean them literally.",
              expression, e.getDescription()),
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
    return regex != null
        ? regex.matcher(test).matches()
        : test.toLowerCase(Locale.ROOT).contains(substring);
  }
}
