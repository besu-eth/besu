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
package org.hyperledger.besu.ethereum.referencetests;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;

/**
 * Maps execution-spec-tests exception keys (e.g. {@code "BlockException.GAS_USED_OVERFLOW"}) to the
 * Besu error-message patterns emitted when that condition is triggered.
 *
 * <p>The mapping is loaded at class-load time from the bundled resource file {@code
 * block-exception-mapping.json}, which mirrors the Python implementation at: <a
 * href="https://github.com/ethereum/execution-specs/blob/mainnet/packages/testing/src/execution_testing/client_clis/clis/besu.py">...</a>
 *
 * <p>To update the mapping, edit {@code block-exception-mapping.json} only — no Java changes
 * needed.
 */
public final class BlockExceptionMatcher {

  private static final Map<String, String> SUBSTRING_MAP;
  private static final Map<String, Pattern> REGEX_MAP;

  static {
    final Map<String, String> substrings = new HashMap<>();
    final Map<String, Pattern> regexes = new HashMap<>();

    try (InputStream in =
        BlockExceptionMatcher.class
            .getClassLoader()
            .getResourceAsStream("block-exception-mapping.json")) {

      if (in == null) {
        throw new IllegalStateException("block-exception-mapping.json not found on classpath");
      }

      final JsonNode root = new ObjectMapper().readTree(in);

      for (var e : root.path("substring").properties()) {
        substrings.put(e.getKey(), e.getValue().asText());
      }
      for (var e : root.path("regex").properties()) {
        regexes.put(e.getKey(), Pattern.compile(e.getValue().asText()));
      }

    } catch (IOException ex) {
      throw new IllegalStateException("Failed to load block-exception-mapping.json", ex);
    }

    SUBSTRING_MAP = Collections.unmodifiableMap(substrings);
    REGEX_MAP = Collections.unmodifiableMap(regexes);
  }

  private BlockExceptionMatcher() {}

  /**
   * Returns whether {@code actualErrorMessage} matches the error pattern for any of the exception
   * keys in {@code exceptionKeyExpr}.
   *
   * <p>The expression may contain multiple keys separated by {@code |} (e.g. {@code
   * "TransactionException.INSUFFICIENT_ACCOUNT_FUNDS|TransactionException.INTRINSIC_GAS_TOO_LOW"}),
   * meaning the fixture accepts any of those exceptions. Returns {@code true} if the actual error
   * message matches at least one of them.
   *
   * @param exceptionKeyExpr the exception key expression from the fixture
   * @param actualErrorMessage the error message produced by Besu during block processing
   * @return {@code true} if the message matches at least one of the expected patterns
   */
  public static boolean matches(final String exceptionKeyExpr, final String actualErrorMessage) {
    for (final String key : Splitter.on('|').split(exceptionKeyExpr)) {
      if (matchesSingle(key.strip(), actualErrorMessage)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesSingle(final String key, final String actualErrorMessage) {
    final String substringPattern = SUBSTRING_MAP.get(key);
    if (substringPattern != null) {
      return actualErrorMessage.contains(substringPattern);
    }
    final Pattern regexPattern = REGEX_MAP.get(key);
    if (regexPattern != null) {
      return regexPattern.matcher(actualErrorMessage).find();
    }
    return false;
  }

  /**
   * Returns a human-readable description of all expected patterns for the given expression, or
   * empty if every constituent key is unknown.
   */
  public static Optional<String> describeExpected(final String exceptionKeyExpr) {
    final StringBuilder sb = new StringBuilder();
    for (final String key : Splitter.on('|').split(exceptionKeyExpr)) {
      final String k = key.strip();
      final String s = SUBSTRING_MAP.get(k);
      if (s != null) {
        if (!sb.isEmpty()) sb.append(" OR ");
        sb.append("contains \"").append(s).append('"');
        continue;
      }
      final Pattern p = REGEX_MAP.get(k);
      if (p != null) {
        if (!sb.isEmpty()) sb.append(" OR ");
        sb.append("matches /").append(p.pattern()).append('/');
      }
    }
    return sb.isEmpty() ? Optional.empty() : Optional.of(sb.toString());
  }
}
