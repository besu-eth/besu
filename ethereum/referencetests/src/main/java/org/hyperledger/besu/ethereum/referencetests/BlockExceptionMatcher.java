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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
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
 * <p>Two surfaces share the file. {@link #matches} answers for a message produced by block import,
 * which is what the JUnit reference tests see; {@link #matchesEngine} answers for one produced over
 * the Engine API, which is what evmtool's {@code engine-test} sees. The engine path runs the same
 * block processing, so it inherits every block-import mapping and the file's {@code engine} section
 * only carries what {@code engine_newPayloadVX} adds.
 *
 * <p>To update the mapping, edit {@code block-exception-mapping.json} only — no Java changes
 * needed.
 */
public final class BlockExceptionMatcher {

  private static final Map<String, String> SUBSTRING_MAP;
  private static final Map<String, Pattern> REGEX_MAP;
  private static final Map<String, String> ENGINE_SUBSTRING_MAP;
  private static final Map<String, Pattern> ENGINE_REGEX_MAP;

  static {
    try (InputStream in =
        BlockExceptionMatcher.class
            .getClassLoader()
            .getResourceAsStream("block-exception-mapping.json")) {

      if (in == null) {
        throw new IllegalStateException("block-exception-mapping.json not found on classpath");
      }

      final JsonNode root = new ObjectMapper().readTree(in);
      SUBSTRING_MAP = readSubstrings(root);
      REGEX_MAP = readRegexes(root);
      final JsonNode engine = root.path("engine");
      ENGINE_SUBSTRING_MAP = readSubstrings(engine);
      ENGINE_REGEX_MAP = readRegexes(engine);

    } catch (IOException ex) {
      throw new IllegalStateException("Failed to load block-exception-mapping.json", ex);
    }
  }

  // Keys beginning with '_' are the section comments the file is annotated with, not exceptions.
  private static Map<String, String> readSubstrings(final JsonNode section) {
    final Map<String, String> substrings = new HashMap<>();
    for (var e : section.path("substring").properties()) {
      if (!e.getKey().startsWith("_")) {
        substrings.put(e.getKey(), e.getValue().asText());
      }
    }
    return Collections.unmodifiableMap(substrings);
  }

  private static Map<String, Pattern> readRegexes(final JsonNode section) {
    final Map<String, Pattern> regexes = new HashMap<>();
    for (var e : section.path("regex").properties()) {
      if (!e.getKey().startsWith("_")) {
        regexes.put(e.getKey(), Pattern.compile(e.getValue().asText()));
      }
    }
    return Collections.unmodifiableMap(regexes);
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
    return matchesSingle(key, actualErrorMessage, SUBSTRING_MAP, REGEX_MAP);
  }

  private static boolean matchesSingle(
      final String key,
      final String actualErrorMessage,
      final Map<String, String> substrings,
      final Map<String, Pattern> regexes) {
    final String substringPattern = substrings.get(key);
    if (substringPattern != null && actualErrorMessage.contains(substringPattern)) {
      return true;
    }
    final Pattern regexPattern = regexes.get(key);
    return regexPattern != null && regexPattern.matcher(actualErrorMessage).find();
  }

  /**
   * As {@link #matches}, for a message produced over the Engine API rather than by block import.
   *
   * <p>The engine path reaches {@code validateAndProcessBlock} through {@code
   * MergeCoordinator.rememberBlock}, so every block-import mapping applies to it too; the engine
   * section of the mapping file only adds the messages {@code engine_newPayloadVX} produces itself.
   * A message matching either counts.
   *
   * @param exceptionKeyExpr the exception key expression from the fixture, {@code |}-separated
   * @param actualErrorMessage the error message Besu returned with the INVALID status
   * @return {@code true} if the message matches at least one of the expected patterns
   */
  public static boolean matchesEngine(
      final String exceptionKeyExpr, final String actualErrorMessage) {
    for (final String key : Splitter.on('|').split(exceptionKeyExpr)) {
      final String k = key.strip();
      if (matchesSingle(k, actualErrorMessage, ENGINE_SUBSTRING_MAP, ENGINE_REGEX_MAP)
          || matchesSingle(k, actualErrorMessage)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Every exception key whose pattern matches the given message, for diagnostics: an oracle failure
   * is far easier to read when it says what Besu's message did map to.
   *
   * @param actualErrorMessage the error message Besu returned
   * @param includeEngine whether to consult the Engine API section as well
   * @return the matching exception keys, in a stable order, possibly empty
   */
  public static Set<String> matchingExceptions(
      final String actualErrorMessage, final boolean includeEngine) {
    final Set<String> matched = new TreeSet<>();
    if (actualErrorMessage == null) {
      return matched;
    }
    final List<Map<String, String>> substringMaps =
        includeEngine ? List.of(SUBSTRING_MAP, ENGINE_SUBSTRING_MAP) : List.of(SUBSTRING_MAP);
    final List<Map<String, Pattern>> regexMaps =
        includeEngine ? List.of(REGEX_MAP, ENGINE_REGEX_MAP) : List.of(REGEX_MAP);
    for (final Map<String, String> map : substringMaps) {
      map.forEach(
          (key, pattern) -> {
            if (actualErrorMessage.contains(pattern)) {
              matched.add(key);
            }
          });
    }
    for (final Map<String, Pattern> map : regexMaps) {
      map.forEach(
          (key, pattern) -> {
            if (pattern.matcher(actualErrorMessage).find()) {
              matched.add(key);
            }
          });
    }
    return matched;
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
