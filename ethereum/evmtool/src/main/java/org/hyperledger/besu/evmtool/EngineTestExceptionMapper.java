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

import org.hyperledger.besu.ethereum.referencetests.BlockExceptionMatcher;

import java.util.Set;

/**
 * Checks a fixture's expected validation error against the message Besu returned, reproducing
 * hive's strict exception matching: when a fixture expects an {@code INVALID} payload with a
 * specific exception, the message Besu returns must map to that exception.
 *
 * <p>The mapping itself lives in {@code block-exception-mapping.json}, shared with the JUnit
 * reference tests — see {@link BlockExceptionMatcher}. This class only phrases the failure.
 */
final class EngineTestExceptionMapper {

  private EngineTestExceptionMapper() {}

  /**
   * Checks an expected validation error (possibly a {@code |}-separated set of alternatives)
   * against the exceptions Besu's message maps to, mirroring hive's strict matching.
   *
   * @param expectedValidationError the fixture's expected validationError
   * @param besuMessage the message Besu returned with the INVALID status
   * @return {@code null} when the actual error matches one of the expected alternatives, otherwise
   *     a failure reason
   */
  static String mismatch(final String expectedValidationError, final String besuMessage) {
    if (besuMessage != null
        && BlockExceptionMatcher.matchesEngine(expectedValidationError, besuMessage)) {
      return null;
    }
    final Set<String> actual = BlockExceptionMatcher.matchingExceptions(besuMessage, true);
    return String.format(
        "expected validation error %s, but Besu returned %s (\"%s\")",
        expectedValidationError, actual.isEmpty() ? "an unmapped error" : actual, besuMessage);
  }
}
